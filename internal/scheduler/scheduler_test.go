package scheduler

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

func testScheduler(t *testing.T, job store.Job, now time.Time) (*Scheduler, *store.SQLite) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		st.Close()
		t.Fatal(err)
	}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return now }
	return s, st
}

func TestTickSchedulesDueRunAndAdvancesJob(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 31, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	job := store.Job{ID: "job", Name: "minute", Schedule: "* * * * *", Command: "echo ok", Status: "active", NextRunAt: &due}
	s, st := testScheduler(t, job, now)
	defer st.Close()
	if err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%d err=%v", len(runs), err)
	}
	if runs[0].Status != "pending" || runs[0].ScheduledAt.Unix() != due.Unix() {
		t.Fatalf("unexpected run: %#v", runs[0])
	}
	got, err := st.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(time.Minute)
	if got.NextRunAt == nil || !got.NextRunAt.Equal(want) {
		t.Fatalf("next run = %v, want %v", got.NextRunAt, want)
	}
}

func TestRecoverRecordsMissedRunAfterMultipleIntervals(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 31, 0, 0, time.UTC)
	due := now.Add(-3 * time.Minute)
	job := store.Job{ID: "job", Name: "minute", Schedule: "* * * * *", Command: "echo ok", Status: "active", NextRunAt: &due}
	s, st := testScheduler(t, job, now)
	defer st.Close()
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%d err=%v", len(runs), err)
	}
	if runs[0].Status != "missed" || runs[0].ScheduledAt.Unix() != due.Unix() {
		t.Fatalf("unexpected run: %#v", runs[0])
	}
	got, err := st.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt == nil || !got.NextRunAt.After(now) {
		t.Fatalf("next run was not advanced: %v", got.NextRunAt)
	}
}

func TestRecoverLeavesRecentlyDueRunForTick(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 31, 0, 0, time.UTC)
	due := now.Add(-30 * time.Second)
	job := store.Job{ID: "job", Name: "minute", Schedule: "* * * * *", Command: "echo ok", Status: "active", NextRunAt: &due}
	s, st := testScheduler(t, job, now)
	defer st.Close()
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 0 {
		t.Fatalf("Recover runs=%d err=%v", len(runs), err)
	}
	if err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	runs, err = st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 1 || runs[0].Status != "pending" {
		t.Fatalf("Tick runs=%#v err=%v", runs, err)
	}
}

func TestRecoverOneShotPastRun(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 31, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	job := store.Job{ID: "job", Name: "once", Schedule: "once:2026-04-02T12:30:00Z", Command: "echo ok", Status: "active", NextRunAt: &due}
	s, st := testScheduler(t, job, now)
	defer st.Close()
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 1 || runs[0].Status != "missed" {
		t.Fatalf("runs=%#v err=%v", runs, err)
	}
	got, err := st.GetJob(context.Background(), job.ID)
	if err != nil || got.NextRunAt != nil {
		t.Fatalf("job next run=%v err=%v", got.NextRunAt, err)
	}
}
