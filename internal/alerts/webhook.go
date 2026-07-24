package alerts

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

const deliveryTimeout = 5 * time.Second
const maxResponseBytes = 4096

type Webhook struct {
	url      string
	secret   string
	statuses map[string]bool
	logger   *slog.Logger
	client   *http.Client
}

type event struct {
	EventID    string     `json:"event_id"`
	Event      string     `json:"event"`
	JobID      string     `json:"job_id"`
	JobName    string     `json:"job_name"`
	RunID      string     `json:"run_id"`
	Status     string     `json:"status"`
	Attempt    int        `json:"attempt"`
	ExitCode   *int       `json:"exit_code"`
	DurationMS *int64     `json:"duration_ms"`
	Error      string     `json:"error"`
	FinishedAt *time.Time `json:"finished_at"`
}

func New(rawURL, secret string, statuses []string, logger *slog.Logger) (*Webhook, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || strings.Contains(rawURL, "#") {
		return nil, errors.New("invalid webhook URL: require an explicit HTTP or HTTPS URL with a host and no credentials or fragment")
	}
	if len(statuses) == 0 {
		statuses = []string{"dead"}
	}
	allowed := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		switch status {
		case "failed", "dead", "succeeded":
			allowed[status] = true
		default:
			return nil, errors.New("invalid webhook status: allowed statuses are failed, dead, succeeded")
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Webhook{
		url:      rawURL,
		secret:   secret,
		statuses: allowed,
		logger:   logger,
		client: &http.Client{
			Timeout: deliveryTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Accepts reports whether this webhook is configured for the run status.
func (w *Webhook) Accepts(status string) bool { return w.statuses[status] }

// AlertForRun snapshots the delivery-relevant fields of a completed run.
// Commands, schedules, and logs are deliberately excluded.
func AlertForRun(job store.Job, run store.Run) store.Alert {
	finished := run.FinishedAt
	if finished != nil {
		utc := finished.UTC()
		finished = &utc
	}
	return store.Alert{
		ID:         run.ID + ":" + run.Status,
		RunID:      run.ID,
		JobID:      run.JobID,
		JobName:    job.Name,
		Status:     run.Status,
		Attempt:    run.Attempt,
		ExitCode:   run.ExitCode,
		DurationMS: run.DurationMS,
		Error:      run.Error,
		FinishedAt: finished,
	}
}

func (w *Webhook) Notify(ctx context.Context, job store.Job, run store.Run) error {
	return w.NotifyAlert(ctx, AlertForRun(job, run))
}

// NotifyAlert delivers an outbox entry. It uses the same payload, signature,
// timeout, and retry rules as Notify.
func (w *Webhook) NotifyAlert(ctx context.Context, alert store.Alert) error {
	if !w.statuses[alert.Status] {
		return nil
	}
	eventID := alert.ID
	if eventID == "" {
		eventID = alert.RunID + ":" + alert.Status
	}
	ctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	payload := event{
		EventID:    eventID,
		Event:      "run." + alert.Status,
		JobID:      alert.JobID,
		JobName:    alert.JobName,
		RunID:      alert.RunID,
		Status:     alert.Status,
		Attempt:    alert.Attempt,
		ExitCode:   alert.ExitCode,
		DurationMS: alert.DurationMS,
		Error:      alert.Error,
		FinishedAt: alert.FinishedAt,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.New("webhook payload encoding failed")
	}
	signature := ""
	if w.secret != "" {
		mac := hmac.New(sha256.New, []byte(w.secret))
		_, _ = mac.Write(body)
		signature = "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		retry, err := w.deliver(ctx, body, payload.EventID, signature)
		if err == nil || !retry || attempt == 2 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("webhook delivery failed")
}

func (w *Webhook) deliver(ctx context.Context, body []byte, eventID, signature string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return false, errors.New("webhook request creation failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AQS-Event-ID", eventID)
	if signature != "" {
		req.Header.Set("X-AQS-Signature", signature)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return true, errors.New("webhook network delivery failed")
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	_ = resp.Body.Close()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 && resp.StatusCode <= 599,
			fmt.Errorf("webhook delivery returned HTTP %d", resp.StatusCode)
	}
	if readErr != nil {
		return true, errors.New("webhook response read failed")
	}
	return false, nil
}

func (w *Webhook) Handler() func(store.Job, store.Run) {
	return func(job store.Job, run store.Run) {
		ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
		defer cancel()
		if err := w.Notify(ctx, job, run); err != nil {
			w.logger.Warn("webhook delivery failed", "job_id", job.ID, "run_id", run.ID, "status", run.Status)
		}
	}
}
