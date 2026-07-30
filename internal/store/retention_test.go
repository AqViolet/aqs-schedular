package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestPruneRunsPreservesLiveAndPendingAlerts(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			st, alerts, _ := alertTestStore(t, backend)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			old := now.Add(-2 * time.Hour)
			seedAlertRun(t, st, old, "old-succeeded", "succeeded")
			alerting := seedAlertRun(t, st, old, "old-alert", "failed")
			seedAlertRun(t, st, now.Add(-time.Minute), "new-succeeded", "succeeded")
			seedAlertRun(t, st, old, "old-pending", "pending")
			recentFinish := seedAlertRun(t, st, old, "old-scheduled-recent-finish", "succeeded")
			recentFinish.FinishedAt = &now
			if err := st.UpdateRun(ctx, recentFinish); err != nil {
				t.Fatal(err)
			}
			if err := alerts.EnqueueAlert(ctx, Alert{ID: alerting.ID + ":failed", RunID: alerting.ID, JobID: alerting.JobID, JobName: "nightly", Status: "failed", Attempt: 1, CreatedAt: now, NextAttemptAt: now}); err != nil {
				t.Fatal(err)
			}
			retention, ok := st.(RetentionStore)
			if !ok {
				t.Fatal("store does not support retention")
			}
			count, err := retention.PruneRuns(ctx, now.Add(-time.Hour), nil, true)
			if err != nil || count != 1 {
				t.Fatalf("dry run count=%d err=%v", count, err)
			}
			count, err = retention.PruneRuns(ctx, now.Add(-time.Hour), nil, false)
			if err != nil || count != 1 {
				t.Fatalf("prune count=%d err=%v", count, err)
			}
			for _, id := range []string{"old-succeeded"} {
				if _, err := st.GetRun(ctx, id); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("prunable run %s remains: %v", id, err)
				}
			}
			for _, id := range []string{"old-alert", "new-succeeded", "old-pending", "old-scheduled-recent-finish"} {
				if _, err := st.GetRun(ctx, id); err != nil {
					t.Fatalf("preserved run %s missing: %v", id, err)
				}
			}
			claimed, err := alerts.ClaimAlerts(ctx, "owner", now, time.Minute, 10)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("claim alert: %+v err=%v", claimed, err)
			}
			if err := alerts.CompleteAlert(ctx, claimed[0].ID, "owner", now, AlertResult{Delivered: true}); err != nil {
				t.Fatal(err)
			}
			count, err = retention.PruneRuns(ctx, now.Add(-time.Hour), []string{"failed"}, true)
			if err != nil || count != 1 {
				t.Fatalf("post-delivery count=%d err=%v", count, err)
			}
			if _, err := retention.PruneRuns(ctx, now, []string{"pending"}, false); err == nil {
				t.Fatal("accepted pruning a live status")
			}
			if _, err := retention.PruneRuns(ctx, now, []string{"running"}, false); err == nil {
				t.Fatal("accepted pruning a running status")
			}
		})
	}
}
