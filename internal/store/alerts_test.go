package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func alertTestStore(t *testing.T, backend string) (Store, AlertStore, *miniredis.Miniredis) {
	t.Helper()
	if backend == "redis" {
		server := miniredis.RunT(t)
		st, err := OpenRedis("redis://"+server.Addr(), "alert-test")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st, st, server
	}
	st, err := Open(filepath.Join(t.TempDir(), "alerts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st, st, nil
}

func seedAlertRun(t *testing.T, st Store, now time.Time, runID, status string) Run {
	t.Helper()
	ctx := context.Background()
	jobID := "job-" + runID
	if _, err := st.GetJob(ctx, jobID); errors.Is(err, sql.ErrNoRows) {
		job := Job{ID: jobID, Name: "nightly-" + runID, Schedule: "@daily", Command: "echo secret", Status: "active", MaxRetries: 3, CreatedAt: now, UpdatedAt: now}
		if err := st.CreateJob(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	exit, duration := 1, int64(250)
	run := Run{ID: runID, JobID: jobID, Status: status, Attempt: 2, ScheduledAt: now, StartedAt: &now, FinishedAt: &now, ExitCode: &exit, DurationMS: &duration, Error: "boom", CreatedAt: now}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestAlertOutboxLifecycle(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			st, alerts, server := alertTestStore(t, backend)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			run := seedAlertRun(t, st, now, "run-1", "succeeded")
			candidates, err := alerts.AlertCandidates(ctx, now, now.Add(-time.Hour), []string{"succeeded"}, 10)
			if err != nil || len(candidates) != 1 {
				t.Fatalf("candidates=%+v err=%v", candidates, err)
			}
			alert := candidates[0]
			if alert.ID != run.ID+":succeeded" || alert.JobName != "nightly-run-1" || alert.Attempt != 2 || alert.ExitCode == nil || *alert.ExitCode != 1 || alert.FinishedAt == nil {
				t.Fatalf("candidate snapshot = %+v", alert)
			}
			if err := alerts.EnqueueAlert(ctx, alert); err != nil {
				t.Fatal(err)
			}
			if err := alerts.EnqueueAlert(ctx, alert); err != nil {
				t.Fatal(err)
			}
			if candidates, err = alerts.AlertCandidates(ctx, now, now.Add(-time.Hour), []string{"succeeded"}, 10); err != nil || len(candidates) != 0 {
				t.Fatalf("duplicate candidate: %+v err=%v", candidates, err)
			}
			claimed, err := alerts.ClaimAlerts(ctx, "worker-a", now, 30*time.Second, 10)
			if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
				t.Fatalf("claim=%+v err=%v", claimed, err)
			}
			if claimed, err = alerts.ClaimAlerts(ctx, "worker-b", now, 30*time.Second, 10); err != nil || len(claimed) != 0 {
				t.Fatalf("second claim=%+v err=%v", claimed, err)
			}
			retryAt := now.Add(time.Minute)
			if err := alerts.CompleteAlert(ctx, alert.ID, "worker-b", now, AlertResult{RetryAt: &retryAt, LastError: "HTTP 503"}); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("wrong owner completion: %v", err)
			}
			if err := alerts.CompleteAlert(ctx, alert.ID, "worker-a", now, AlertResult{RetryAt: &retryAt, LastError: "HTTP 503"}); err != nil {
				t.Fatal(err)
			}
			if claimed, err = alerts.ClaimAlerts(ctx, "worker-c", now.Add(30*time.Second), 30*time.Second, 10); err != nil || len(claimed) != 0 {
				t.Fatalf("early retry claim=%+v err=%v", claimed, err)
			}
			if server != nil {
				server.FastForward(61 * time.Second)
			}
			claimed, err = alerts.ClaimAlerts(ctx, "worker-c", retryAt, 30*time.Second, 10)
			if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 || claimed[0].LastError != "HTTP 503" {
				t.Fatalf("retry claim=%+v err=%v", claimed, err)
			}
			if err := alerts.CompleteAlert(ctx, alert.ID, "worker-c", retryAt, AlertResult{Delivered: true}); err != nil {
				t.Fatal(err)
			}
			if claimed, err = alerts.ClaimAlerts(ctx, "worker-d", retryAt.Add(time.Hour), 30*time.Second, 10); err != nil || len(claimed) != 0 {
				t.Fatalf("delivered claim=%+v err=%v", claimed, err)
			}
			_ = server
		})
	}
}

func TestAlertOutboxLeaseExpiryAndAbandon(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			st, alerts, server := alertTestStore(t, backend)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			_ = seedAlertRun(t, st, now, "run-lease", "dead")
			candidates, err := alerts.AlertCandidates(ctx, now, now.Add(-time.Hour), []string{"dead"}, 10)
			if err != nil || len(candidates) != 1 {
				t.Fatalf("candidates=%+v err=%v", candidates, err)
			}
			if err := alerts.EnqueueAlert(ctx, candidates[0]); err != nil {
				t.Fatal(err)
			}
			claimed, err := alerts.ClaimAlerts(ctx, "worker-a", now, time.Second, 10)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("claim=%+v err=%v", claimed, err)
			}
			if server != nil {
				server.FastForward(2 * time.Second)
			}
			claimed, err = alerts.ClaimAlerts(ctx, "worker-b", now.Add(2*time.Second), 30*time.Second, 10)
			if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
				t.Fatalf("expired claim=%+v err=%v", claimed, err)
			}
			if err := alerts.CompleteAlert(ctx, claimed[0].ID, "worker-b", now.Add(2*time.Second), AlertResult{LastError: "permanent"}); err != nil {
				t.Fatal(err)
			}
			if claimed, err = alerts.ClaimAlerts(ctx, "worker-c", now.Add(time.Hour), 30*time.Second, 10); err != nil || len(claimed) != 0 {
				t.Fatalf("abandoned claim=%+v err=%v", claimed, err)
			}
		})
	}
}

func TestAlertCandidatesFiltersAndJobDeletion(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			st, alerts, _ := alertTestStore(t, backend)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			run := seedAlertRun(t, st, now, "run-delete", "failed")
			if candidates, err := alerts.AlertCandidates(ctx, now, now.Add(time.Hour), []string{"failed"}, 10); err != nil || len(candidates) != 0 {
				t.Fatalf("future candidate: %+v err=%v", candidates, err)
			}
			if candidates, err := alerts.AlertCandidates(ctx, now, now.Add(-2*time.Hour), []string{"failed"}, 10); err != nil || len(candidates) != 1 {
				t.Fatalf("failed candidate: %+v err=%v", candidates, err)
			} else if err := alerts.EnqueueAlert(ctx, candidates[0]); err != nil {
				t.Fatal(err)
			}
			if err := st.DeleteJob(ctx, run.JobID); err != nil {
				t.Fatal(err)
			}
			if claimed, err := alerts.ClaimAlerts(ctx, "worker", now, 30*time.Second, 10); err != nil || len(claimed) != 0 {
				t.Fatalf("claim after job delete=%+v err=%v", claimed, err)
			}
		})
	}
}
