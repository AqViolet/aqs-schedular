package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

func TestCommandLifecycle(t *testing.T) {
	db := filepath.Join(t.TempDir(), "scheduler.db")
	t.Setenv("AQS_DB", db)
	var out, stderr bytes.Buffer
	if err := run([]string{"add", "--schedule", "@every 1m", "--name", "sample", "--timeout", "10", "--max-retries", "2", "--backoff", "linear", "--", "printf", "hello"}, &out, &stderr); err != nil {
		t.Fatalf("add: %v (%s)", err, stderr.String())
	}
	jobID := strings.TrimSpace(out.String())
	if jobID == "" {
		t.Fatal("add returned an empty job ID")
	}

	out.Reset()
	if err := run([]string{"list", "--status", "active"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "sample") || !strings.Contains(out.String(), "@every 1m") || !strings.Contains(out.String(), "printf hello") {
		t.Fatalf("list output: %s", out.String())
	}

	out.Reset()
	if err := run([]string{"run", jobID}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	runID := strings.TrimSpace(out.String())
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AppendLog(context.Background(), store.Log{RunID: runID, Stream: "stdout", Chunk: "hello from run\n", Seq: 1, CreatedAt: time.Now()}); err != nil {
		st.Close()
		t.Fatal(err)
	}
	st.Close()

	out.Reset()
	if err := run([]string{"logs", jobID, "--run", runID}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if out.String() != "[stdout] hello from run\n" {
		t.Fatalf("logs output %q", out.String())
	}

	out.Reset()
	if err := run([]string{"pause", jobID}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run([]string{"list", "--status", "paused"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "sample") {
		t.Fatalf("paused job missing: %s", out.String())
	}
	if err := run([]string{"resume", jobID}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"delete", jobID}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run([]string{"list"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), jobID) {
		t.Fatalf("deleted job still listed: %s", out.String())
	}
}

func TestAddOneShotAndValidation(t *testing.T) {
	db := filepath.Join(t.TempDir(), "scheduler.db")
	t.Setenv("AQS_DB", db)
	var out, stderr bytes.Buffer
	if err := run([]string{"add", "--at", "2026-10-01T15:00:00Z", "--name", "later", "--", "echo", "done"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	jobID := strings.TrimSpace(out.String())
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	job, err := st.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Schedule != "once:2026-10-01T15:00:00Z" || job.NextRunAt == nil || !job.NextRunAt.Equal(time.Date(2026, 10, 1, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("one-shot job = %#v", job)
	}
	if err := run([]string{"add", "--name", "bad", "--", "echo"}, &out, &stderr); err == nil {
		t.Fatal("add without a schedule should fail")
	}
}

func TestPruneRunsPreservesLiveAndAlertingHistory(t *testing.T) {
	db := filepath.Join(t.TempDir(), "scheduler.db")
	t.Setenv("AQS_DB", db)
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	job := store.Job{ID: "job", Name: "nightly", Schedule: "@daily", Command: "echo ok", Status: "active", CreatedAt: now, UpdatedAt: now}
	if err := st.CreateJob(context.Background(), job); err != nil {
		st.Close()
		t.Fatal(err)
	}
	add := func(id, status string, at time.Time) {
		t.Helper()
		if err := st.CreateRun(context.Background(), store.Run{ID: id, JobID: job.ID, Status: status, Attempt: 1, ScheduledAt: at, CreatedAt: at}); err != nil {
			st.Close()
			t.Fatal(err)
		}
	}
	add("old-succeeded", "succeeded", now.Add(-2*time.Hour))
	add("old-failed-alerting", "failed", now.Add(-2*time.Hour))
	add("old-pending", "pending", now.Add(-2*time.Hour))
	add("recent-succeeded", "succeeded", now.Add(-time.Minute))
	alerts, ok := any(st).(store.AlertStore)
	if !ok {
		st.Close()
		t.Fatal("store does not support alerts")
	}
	if err := alerts.EnqueueAlert(context.Background(), store.Alert{ID: "old-failed-alerting:failed", RunID: "old-failed-alerting", JobID: job.ID, JobName: job.Name, Status: "failed", Attempt: 1}); err != nil {
		st.Close()
		t.Fatal(err)
	}
	st.Close()

	var out, stderr bytes.Buffer
	if err := run([]string{"prune", "--older-than", "1h", "--dry-run"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "would prune 1 runs") {
		t.Fatalf("dry run output: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"prune", "--older-than", "1h"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "pruned 1 runs") {
		t.Fatalf("prune output: %s", out.String())
	}
	st, err = store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool)
	for _, run := range runs {
		got[run.ID] = true
	}
	if got["old-succeeded"] || !got["old-failed-alerting"] || !got["old-pending"] || !got["recent-succeeded"] {
		t.Fatalf("unexpected runs after prune: %+v", got)
	}
}
