package alerts

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

func outboxFixture(t *testing.T, url string) (*Outbox, store.Store, *store.SQLite, store.Job, store.Run, time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	job := store.Job{ID: "job", Name: "nightly", Schedule: "@daily", Command: "echo private-command", Status: "active", MaxRetries: 2, CreatedAt: now, UpdatedAt: now}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	exit, duration := 1, int64(50)
	run := store.Run{ID: "run", JobID: job.ID, Status: "dead", Attempt: 2, ScheduledAt: now, StartedAt: &now, FinishedAt: &now, ExitCode: &exit, DurationMS: &duration, Error: "exit status 1", CreatedAt: now}
	if err := st.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	webhook := newWebhook(t, url, "", nil)
	cfg := DefaultOutboxConfig()
	cfg.MaxAttempts = 2
	cfg.RetryBase = 100 * time.Millisecond
	cfg.RetryMax = time.Second
	cfg.Lease = time.Second
	cfg.Poll = 100 * time.Millisecond
	cfg.SweepEvery = 100 * time.Millisecond
	cfg.SweepLookback = time.Hour
	cfg.ClaimLimit, cfg.SweepLimit = 10, 10
	outbox, err := NewOutbox(st, webhook, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	outbox.now = func() time.Time { return now }
	return outbox, st, st, job, run, now
}

func TestOutboxDeliversSweptAndHandlerAlerts(t *testing.T) {
	var calls atomic.Int32
	var lastBody atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lastBody.Store(string(body))
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	outbox, st, _, job, run, now := outboxFixture(t, server.URL)

	outbox.SweepOnce(context.Background())
	outbox.DeliverOnce(context.Background())
	if calls.Load() != 1 {
		t.Fatalf("swept delivery calls = %d", calls.Load())
	}
	body, _ := lastBody.Load().(string)
	for _, private := range []string{"private-command", "@daily", "logs"} {
		if len(body) > 0 && strings.Contains(body, private) {
			t.Fatalf("payload leaked %q: %s", private, body)
		}
	}
	if alerts, err := st.(store.AlertStore).ClaimAlerts(context.Background(), "other", now, time.Second, 10); err != nil || len(alerts) != 0 {
		t.Fatalf("delivered alert was reclaimable: %+v err=%v", alerts, err)
	}

	outbox.Handler()(job, run)
	outbox.DeliverOnce(context.Background())
	if calls.Load() != 1 {
		t.Fatalf("duplicate handler enqueue delivered: %d", calls.Load())
	}

	second := run
	second.ID, second.Status = "run-2", "succeeded"
	if err := st.CreateRun(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	outbox.Handler()(job, store.Run{ID: "run-3", JobID: job.ID, Status: "running"})
	outbox.DeliverOnce(context.Background())
	if calls.Load() != 1 {
		t.Fatalf("non-terminal status delivered: %d", calls.Load())
	}
}

func TestOutboxRetriesThenDelivers(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	outbox, st, _, _, _, now := outboxFixture(t, server.URL)
	ctx := context.Background()
	outbox.SweepOnce(ctx)
	outbox.DeliverOnce(ctx)
	if calls.Load() == 0 {
		t.Fatal("first delivery was not attempted")
	}
	if alerts, err := st.(store.AlertStore).ClaimAlerts(ctx, "other", now.Add(-time.Second), time.Second, 10); err != nil || len(alerts) != 0 {
		t.Fatalf("retry was immediately claimable: %+v err=%v", alerts, err)
	}
	outbox.now = func() time.Time { return now.Add(2 * time.Second) }
	outbox.DeliverOnce(ctx)
	alerts, err := st.(store.AlertStore).ClaimAlerts(ctx, "other", now.Add(time.Hour), time.Second, 10)
	if err != nil || len(alerts) != 0 {
		t.Fatalf("delivered retry still pending: %+v err=%v", alerts, err)
	}
}

func TestOutboxAbandonsAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	outbox, st, _, _, _, now := outboxFixture(t, server.URL)
	ctx := context.Background()
	outbox.SweepOnce(ctx)
	outbox.DeliverOnce(ctx)
	outbox.now = func() time.Time { return now.Add(2 * time.Second) }
	outbox.DeliverOnce(ctx)
	afterAbandon := calls.Load()
	outbox.now = func() time.Time { return now.Add(time.Hour) }
	outbox.DeliverOnce(ctx)
	if calls.Load() != afterAbandon {
		t.Fatalf("abandoned alert was retried: %d -> %d", afterAbandon, calls.Load())
	}
	if alerts, err := st.(store.AlertStore).ClaimAlerts(ctx, "other", now.Add(2*time.Hour), time.Second, 10); err != nil || len(alerts) != 0 {
		t.Fatalf("abandoned alert claimable: %+v err=%v", alerts, err)
	}
}

func TestOutboxConfigValidation(t *testing.T) {
	webhook := newWebhook(t, "http://example.com", "", nil)
	cfg := DefaultOutboxConfig()
	cfg.MaxAttempts = 0
	if _, err := NewOutbox(nil, webhook, cfg, nil); err == nil {
		t.Fatal("accepted nil store")
	}
	if _, err := NewOutbox(alertStoreStub{}, nil, DefaultOutboxConfig(), nil); err == nil {
		t.Fatal("accepted nil webhook")
	}
	for _, mutate := range []func(*OutboxConfig){
		func(c *OutboxConfig) { c.MaxAttempts = 0 },
		func(c *OutboxConfig) { c.RetryBase = 0 },
		func(c *OutboxConfig) { c.Lease = 0 },
		func(c *OutboxConfig) { c.Poll = 0 },
		func(c *OutboxConfig) { c.SweepLookback = 0 },
	} {
		cfg := DefaultOutboxConfig()
		mutate(&cfg)
		if _, err := NewOutbox(alertStoreStub{}, webhook, cfg, nil); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
}

type alertStoreStub struct{}

func (alertStoreStub) EnqueueAlert(context.Context, store.Alert) error { return nil }
func (alertStoreStub) ClaimAlerts(context.Context, string, time.Time, time.Duration, int) ([]store.Alert, error) {
	return nil, nil
}
func (alertStoreStub) CompleteAlert(context.Context, string, string, time.Time, store.AlertResult) error {
	return nil
}
func (alertStoreStub) AlertCandidates(context.Context, time.Time, time.Time, []string, int) ([]store.Alert, error) {
	return nil, nil
}
