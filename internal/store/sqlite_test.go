package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteCRUDAndRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 4, 2, 12, 30, 0, 0, time.UTC)
	next := now.Add(time.Minute)
	job := Job{
		ID: "job-1", Name: "ping", Schedule: "* * * * *", Command: "echo ping",
		TimeoutSeconds: 10, MaxRetries: 3, Backoff: "exponential", Status: "active",
		NextRunAt: &next, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID || got.Name != job.Name || got.Schedule != job.Schedule || got.Command != job.Command || got.TimeoutSeconds != 10 || got.MaxRetries != 3 || got.Backoff != "exponential" || got.Status != "active" || got.NextRunAt == nil || !got.NextRunAt.Equal(next) || !got.CreatedAt.Equal(now) {
		t.Fatalf("job mismatch: %#v", got)
	}

	jobs, err := s.ListJobs(ctx, "active")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs: jobs=%d err=%v", len(jobs), err)
	}
	due, err := s.DueJobs(ctx, next)
	if err != nil || len(due) != 1 || due[0].ID != job.ID {
		t.Fatalf("DueJobs: jobs=%d err=%v", len(due), err)
	}
	if due, err = s.DueJobs(ctx, next.Add(-time.Second)); err != nil || len(due) != 0 {
		t.Fatalf("DueJobs before schedule: jobs=%d err=%v", len(due), err)
	}

	run := Run{ID: "run-1", JobID: job.ID, Status: "running", Attempt: 1, ScheduledAt: next, CreatedAt: next}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	started := next.Add(time.Second)
	exitCode := 0
	duration := int64(1000)
	finished := started.Add(time.Duration(duration) * time.Millisecond)
	run.Status, run.StartedAt, run.FinishedAt, run.ExitCode, run.DurationMS = "succeeded", &started, &finished, &exitCode, &duration
	if err := s.UpdateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	gotRun, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRun.Status != "succeeded" || gotRun.ExitCode == nil || *gotRun.ExitCode != 0 || gotRun.DurationMS == nil || *gotRun.DurationMS != duration || gotRun.StartedAt == nil || !gotRun.StartedAt.Equal(started) || gotRun.FinishedAt == nil || !gotRun.FinishedAt.Equal(finished) {
		t.Fatalf("run mismatch: %#v", gotRun)
	}

	logs := []Log{
		{RunID: run.ID, Stream: "stdout", Chunk: "hello\n", Seq: 1, CreatedAt: started},
		{RunID: run.ID, Stream: "stderr", Chunk: "warning\n", Seq: 2, CreatedAt: finished},
	}
	for _, l := range logs {
		if err := s.AppendLog(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	gotLogs, err := s.ListLogs(ctx, run.ID)
	if err != nil || len(gotLogs) != 2 || gotLogs[0].Chunk != "hello\n" || gotLogs[1].Stream != "stderr" {
		t.Fatalf("ListLogs: logs=%#v err=%v", gotLogs, err)
	}

	if err := s.SetJobStatus(ctx, job.ID, "paused"); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetJob(ctx, job.ID)
	if err != nil || got.Status != "paused" {
		t.Fatalf("SetJobStatus: job=%#v err=%v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	gotRun, err = s.GetRun(ctx, run.ID)
	if err != nil || gotRun.Status != "succeeded" || gotRun.ExitCode == nil || *gotRun.ExitCode != 0 {
		t.Fatalf("recovered run: %#v err=%v", gotRun, err)
	}
	gotLogs, err = s.ListLogs(ctx, run.ID)
	if err != nil || len(gotLogs) != 2 {
		t.Fatalf("recovered logs: %#v err=%v", gotLogs, err)
	}
	if err := s.DeleteJob(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetJob(ctx, job.ID); err != sql.ErrNoRows {
		t.Fatalf("deleted job lookup error = %v, want sql.ErrNoRows", err)
	}
	if runs, err := s.ListRuns(ctx, job.ID); err != nil || len(runs) != 0 {
		t.Fatalf("delete cascade runs=%d err=%v", len(runs), err)
	}
}

func TestSQLiteMigrationIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scheduler.db")
	for i := 0; i < 3; i++ {
		s, err := Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteMissingRows(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.GetJob(context.Background(), "missing"); err != sql.ErrNoRows {
		t.Fatalf("GetJob error = %v, want sql.ErrNoRows", err)
	}
	if err := s.SetJobStatus(context.Background(), "missing", "paused"); err != sql.ErrNoRows {
		t.Fatalf("SetJobStatus error = %v, want sql.ErrNoRows", err)
	}
}
