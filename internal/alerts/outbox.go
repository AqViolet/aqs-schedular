package alerts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

// OutboxConfig controls the durable webhook delivery worker.
type OutboxConfig struct {
	MaxAttempts   int
	RetryBase     time.Duration
	RetryMax      time.Duration
	Lease         time.Duration
	Poll          time.Duration
	SweepEvery    time.Duration
	SweepLookback time.Duration
	ClaimLimit    int
	SweepLimit    int
}

func DefaultOutboxConfig() OutboxConfig {
	return OutboxConfig{
		MaxAttempts:   8,
		RetryBase:     2 * time.Second,
		RetryMax:      10 * time.Minute,
		Lease:         30 * time.Second,
		Poll:          time.Second,
		SweepEvery:    time.Minute,
		SweepLookback: 24 * time.Hour,
		ClaimLimit:    20,
		SweepLimit:    100,
	}
}

// Outbox persists alert deliveries before attempting them, retries transient
// failures, and reconciles terminal runs that were completed while a worker
// was unavailable.
type Outbox struct {
	store   store.AlertStore
	webhook *Webhook
	logger  *slog.Logger
	cfg     OutboxConfig
	owner   string
	now     func() time.Time
}

func NewOutbox(st store.AlertStore, webhook *Webhook, cfg OutboxConfig, logger *slog.Logger) (*Outbox, error) {
	if st == nil {
		return nil, errors.New("alert outbox requires a store")
	}
	if webhook == nil {
		return nil, errors.New("alert outbox requires a webhook")
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > 100 {
		return nil, errors.New("alert max attempts must be between 1 and 100")
	}
	if cfg.RetryBase < 100*time.Millisecond || cfg.RetryBase > time.Hour {
		return nil, errors.New("alert retry base must be between 100ms and 1h")
	}
	if cfg.RetryMax < cfg.RetryBase || cfg.RetryMax > 24*time.Hour {
		return nil, errors.New("alert retry maximum must be at least the base and at most 24h")
	}
	if cfg.Lease < time.Second || cfg.Lease > time.Hour {
		return nil, errors.New("alert lease must be between 1s and 1h")
	}
	if cfg.Poll < 100*time.Millisecond || cfg.Poll > time.Minute {
		return nil, errors.New("alert poll interval must be between 100ms and 1m")
	}
	if cfg.SweepEvery < cfg.Poll || cfg.SweepEvery > 24*time.Hour {
		return nil, errors.New("alert sweep interval must be between the poll interval and 24h")
	}
	if cfg.SweepLookback < time.Minute || cfg.SweepLookback > 90*24*time.Hour {
		return nil, errors.New("alert sweep lookback must be between 1m and 2160h")
	}
	if cfg.ClaimLimit < 1 || cfg.ClaimLimit > 1000 || cfg.SweepLimit < 1 || cfg.SweepLimit > 10000 {
		return nil, errors.New("alert claim and sweep limits are out of range")
	}
	if logger == nil {
		logger = slog.Default()
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	return &Outbox{
		store: st, webhook: webhook, logger: logger, cfg: cfg,
		owner: token, now: time.Now,
	}, nil
}

// Handler enqueues an alert after the executor persists a terminal run.
func (o *Outbox) Handler() func(store.Job, store.Run) {
	return func(job store.Job, run store.Run) {
		if !o.webhook.Accepts(run.Status) {
			return
		}
		alert := AlertForRun(job, run)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := o.store.EnqueueAlert(ctx, alert); err != nil {
			o.logger.Warn("enqueue alert failed", "run_id", run.ID, "status", run.Status)
		}
	}
}

// Run delivers due alerts and periodically reconciles missed enqueues.
func (o *Outbox) Run(ctx context.Context) error {
	o.SweepOnce(ctx)
	o.DeliverOnce(ctx)
	poll := time.NewTicker(o.cfg.Poll)
	defer poll.Stop()
	sweep := time.NewTicker(o.cfg.SweepEvery)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
			o.DeliverOnce(ctx)
		case <-sweep.C:
			o.SweepOnce(ctx)
		}
	}
}

// SweepOnce enqueues terminal runs that have no outbox row. This covers a
// crash between result persistence and the executor's enqueue callback.
func (o *Outbox) SweepOnce(ctx context.Context) {
	now := o.now().UTC()
	candidates, err := o.store.AlertCandidates(ctx, now, now.Add(-o.cfg.SweepLookback), o.webhook.Statuses(), o.cfg.SweepLimit)
	if err != nil {
		o.logger.Warn("alert outbox sweep failed")
		return
	}
	enqueued := 0
	for _, alert := range candidates {
		if err := o.store.EnqueueAlert(ctx, alert); err != nil {
			o.logger.Warn("alert outbox enqueue failed", "run_id", alert.RunID, "status", alert.Status)
			continue
		}
		enqueued++
	}
	if enqueued > 0 {
		o.logger.Info("alert outbox reconciled", "count", enqueued)
	}
}

// DeliverOnce claims and attempts one batch of due alerts.
func (o *Outbox) DeliverOnce(ctx context.Context) {
	now := o.now().UTC()
	alerts, err := o.store.ClaimAlerts(ctx, o.owner, now, o.cfg.Lease, o.cfg.ClaimLimit)
	if err != nil {
		o.logger.Warn("alert outbox claim failed")
		return
	}
	for _, alert := range alerts {
		if ctx.Err() != nil {
			return
		}
		o.deliver(ctx, alert)
	}
}

func (o *Outbox) deliver(ctx context.Context, alert store.Alert) {
	err := o.webhook.NotifyAlert(ctx, alert)
	if err == nil {
		o.complete(alert, store.AlertResult{Delivered: true})
		return
	}
	if alert.Attempts >= o.cfg.MaxAttempts {
		o.complete(alert, store.AlertResult{LastError: err.Error()})
		o.logger.Warn("alert abandoned after delivery attempts", "alert_id", alert.ID, "attempts", alert.Attempts)
		return
	}
	retryAt := o.now().UTC().Add(o.backoff(alert.Attempts))
	o.complete(alert, store.AlertResult{RetryAt: &retryAt, LastError: err.Error()})
	o.logger.Warn("alert delivery failed", "alert_id", alert.ID, "attempts", alert.Attempts, "retry_at", retryAt)
}

func (o *Outbox) complete(alert store.Alert, result store.AlertResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.store.CompleteAlert(ctx, alert.ID, o.owner, o.now().UTC(), result); err != nil {
		o.logger.Warn("alert outbox completion failed", "alert_id", alert.ID)
	}
}

func (o *Outbox) backoff(attempts int) time.Duration {
	delay := o.cfg.RetryBase
	for i := 1; i < attempts; i++ {
		if delay >= o.cfg.RetryMax/2 {
			return o.cfg.RetryMax
		}
		delay *= 2
	}
	if delay > o.cfg.RetryMax {
		return o.cfg.RetryMax
	}
	return delay
}

func (w *Webhook) Statuses() []string {
	statuses := make([]string, 0, len(w.statuses))
	for status := range w.statuses {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	return statuses
}

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate alert owner token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
