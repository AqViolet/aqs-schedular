package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestJobConfigurationUpdates(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var s Store
			if backend == "redis" {
				s, _ = testRedis(t)
			} else {
				var err error
				s, err = Open(filepath.Join(t.TempDir(), "store.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
			}
			configuration, ok := s.(JobConfigurationStore)
			if !ok {
				t.Fatal("store does not support configuration updates")
			}
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
			next := now.Add(time.Minute)
			job := Job{ID: "job", Name: "original", Schedule: "* * * * *", Command: "echo original", TimeoutSeconds: 10, MaxRetries: 1, Backoff: "none", Status: "active", NextRunAt: &now, CreatedAt: now, UpdatedAt: now}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatal(err)
			}
			stale, err := s.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			run := Run{ID: "run", JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: now}
			if err := s.ScheduleJobRun(ctx, job.ID, run, &next); err != nil {
				t.Fatal(err)
			}
			if err := s.SetJobStatus(ctx, job.ID, "paused"); err != nil {
				t.Fatal(err)
			}
			stale.Name, stale.Command = "edited", "echo edited"
			stale.TimeoutSeconds, stale.MaxRetries, stale.Backoff = 30, 3, "exponential"
			stale.CreatedAt, stale.UpdatedAt = next, now.Add(-time.Hour)
			if err := configuration.UpdateJobConfiguration(ctx, stale, false); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != stale.Name || got.Schedule != stale.Schedule || got.Command != stale.Command || got.TimeoutSeconds != stale.TimeoutSeconds || got.MaxRetries != stale.MaxRetries || got.Backoff != stale.Backoff {
				t.Fatalf("configuration not updated: %+v", got)
			}
			if got.Status != "paused" || !got.CreatedAt.Equal(now) || got.NextRunAt == nil || !got.NextRunAt.Equal(next) || !got.UpdatedAt.After(now) {
				t.Fatalf("stale snapshot overwrote current state: %+v", got)
			}
			rescheduled := next.Add(time.Hour)
			stale.Schedule, stale.NextRunAt = "0 * * * *", &rescheduled
			if err := configuration.UpdateJobConfiguration(ctx, stale, true); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.Schedule != stale.Schedule || got.NextRunAt == nil || !got.NextRunAt.Equal(rescheduled) || got.Status != "paused" || !got.CreatedAt.Equal(now) {
				t.Fatalf("changed schedule: %+v err=%v", got, err)
			}
			stale.NextRunAt = nil
			if err := configuration.UpdateJobConfiguration(ctx, stale, false); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.NextRunAt == nil || !got.NextRunAt.Equal(rescheduled) {
				t.Fatalf("unchanged schedule cleared next run: %+v err=%v", got, err)
			}
			if err := configuration.UpdateJobConfiguration(ctx, stale, true); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.NextRunAt != nil || got.Status != "paused" || !got.CreatedAt.Equal(now) {
				t.Fatalf("explicitly cleared schedule: %+v err=%v", got, err)
			}
			if err := s.SetJobStatus(ctx, job.ID, "active"); err != nil {
				t.Fatal(err)
			}
			stale.NextRunAt = &rescheduled
			if err := configuration.UpdateJobConfiguration(ctx, stale, false); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.NextRunAt != nil || got.Status != "active" {
				t.Fatalf("unchanged schedule restored completed next run: %+v err=%v", got, err)
			}
			for _, changed := range []bool{false, true} {
				if err := configuration.UpdateJobConfiguration(ctx, Job{ID: "missing"}, changed); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("missing job, changed=%v: %v", changed, err)
				}
			}
		})
	}
}
