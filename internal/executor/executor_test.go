package executor

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

func newTestStore(t *testing.T) *store.SQLite {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func createRun(t *testing.T, st *store.SQLite, command string, timeout, retries int) (store.Job, store.Run) {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	job := store.Job{
		ID: "job", Name: "test", Schedule: "* * * * *", Command: command,
		TimeoutSeconds: timeout, MaxRetries: retries, Backoff: "none", Status: "active",
		CreatedAt: now, UpdatedAt: now,
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	run := store.Run{ID: "run", JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: now, CreatedAt: now}
	if err := st.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	return job, run
}

func testPool(st store.Store) *Pool {
	return New(st, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestExecutorCapturesStreamsAndExitCode(t *testing.T) {
	st := newTestStore(t)
	job, run := createRun(t, st, "printf 'hello stdout'; printf 'hello stderr' >&2; exit 7", 5, 1)
	p := testPool(st)
	p.process(context.Background(), run)

	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "dead" || got.ExitCode == nil || *got.ExitCode != 7 || got.DurationMS == nil || got.FinishedAt == nil {
		t.Fatalf("run = %#v", got)
	}
	logs, err := st.ListLogs(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr string
	for _, l := range logs {
		if l.Stream == "stdout" {
			stdout += l.Chunk
		}
		if l.Stream == "stderr" {
			stderr += l.Chunk
		}
		if len(l.Chunk) > logChunkSize {
			t.Fatalf("chunk exceeded size: %d", len(l.Chunk))
		}
	}
	if stdout != "hello stdout" || stderr != "hello stderr" {
		t.Fatalf("stdout=%q stderr=%q", stdout, stderr)
	}
	if job.ID != got.JobID {
		t.Fatalf("job mismatch: %#v", got)
	}
}

func TestExecutorChunksOutput(t *testing.T) {
	st := newTestStore(t)
	_, run := createRun(t, st, "printf '%02000d' 0", 5, 1)
	p := testPool(st)
	p.process(context.Background(), run)
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "succeeded" || got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("successful run = %#v err=%v", got, err)
	}
	logs, err := st.ListLogs(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || len(logs[0].Chunk) != logChunkSize || len(logs[1].Chunk) != 2000-logChunkSize {
		t.Fatalf("unexpected chunks: %#v", logs)
	}
}

func TestPoolDispatchAndGracefulShutdown(t *testing.T) {
	st := newTestStore(t)
	_, run := createRun(t, st, "printf dispatched", 5, 1)
	p := New(st, 2, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.poll = 10 * time.Millisecond
	p.Start()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == "succeeded" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "succeeded" {
		t.Fatalf("run = %#v err=%v", got, err)
	}
}

func TestExecutorTimeout(t *testing.T) {
	st := newTestStore(t)
	_, run := createRun(t, st, "sleep 20", 1, 1)
	p := testPool(st)
	started := time.Now()
	p.process(context.Background(), run)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("timeout returned too slowly: %s", elapsed)
	}
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "dead" || got.Error != "timeout after 1s" || got.FinishedAt == nil {
		t.Fatalf("timeout result = %#v", got)
	}
}

func TestExecutorSchedulesRetry(t *testing.T) {
	st := newTestStore(t)
	_, run := createRun(t, st, "exit 1", 5, 2)
	p := testPool(st)
	p.process(context.Background(), run)
	runs, err := st.ListRuns(context.Background(), run.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want failed run and retry", len(runs))
	}
	var failed, retry *store.Run
	for i := range runs {
		r := &runs[i]
		if r.Attempt == 1 {
			failed = r
		}
		if r.Attempt == 2 {
			retry = r
		}
	}
	if failed == nil || failed.Status != "failed" || failed.ExitCode == nil || *failed.ExitCode != 1 {
		t.Fatalf("first attempt = %#v", failed)
	}
	if retry == nil || retry.Status != "pending" || retry.ScheduledAt.Before(time.Now().Add(-time.Second)) {
		t.Fatalf("retry = %#v", retry)
	}
}

func TestBackoffDelay(t *testing.T) {
	rngA := rand.New(rand.NewSource(42))
	rngB := rand.New(rand.NewSource(42))
	for attempt := 1; attempt <= 8; attempt++ {
		a := BackoffDelay("exponential", attempt, rngA)
		b := BackoffDelay("exponential", attempt, rngB)
		base := time.Second * time.Duration(uint64(1)<<uint(attempt-1))
		if a != b || a < base || a >= base+time.Second {
			t.Fatalf("attempt %d delay %s, deterministic %s, base %s", attempt, a, b, base)
		}
	}
	if got := BackoffDelay("linear", 3, nil); got != 3*time.Second {
		t.Fatalf("linear = %s", got)
	}
	if got := BackoffDelay("none", 3, nil); got != 0 {
		t.Fatalf("none = %s", got)
	}
	if got := BackoffDelay("unknown", 1, nil); got != time.Second {
		t.Fatalf("default exponential = %s", got)
	}
}

func TestLogChunkWriter(t *testing.T) {
	st := newTestStore(t)
	_, run := createRun(t, st, "true", 5, 1)
	seq := &logSequence{store: st, runID: run.ID}
	writer := &chunkWriter{stream: "stdout", sequence: seq}
	input := strings.Repeat("x", 2*logChunkSize+17)
	if _, err := writer.Write([]byte(input)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	logs, err := st.ListLogs(context.Background(), run.ID)
	if err != nil || len(logs) != 3 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	var output strings.Builder
	for _, l := range logs {
		output.WriteString(l.Chunk)
	}
	if output.String() != input {
		t.Fatalf("output length=%d, want %d", output.Len(), len(input))
	}
}
