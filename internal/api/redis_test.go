package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func setupBackend(t *testing.T, backend string) (store.Store, http.Handler) {
	t.Helper()
	if backend == "sqlite" {
		return setup(t)
	}
	server := miniredis.RunT(t)
	st, err := store.OpenRedis("redis://"+server.Addr(), "api-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, New(st, testToken, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSQLiteRedisResponseParity(t *testing.T) {
	var baseline []map[string]any
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			st, handler := setupBackend(t, backend)
			createdResponse := request(t, handler, "POST", "/api/jobs", `{"name":"parity","schedule":"once:2099-01-02T03:04:05Z","command":"echo parity","timeout_seconds":5,"max_retries":2,"backoff":"linear"}`, 201)
			job := decode[jobDTO](t, createdResponse)
			created := decode[map[string]any](t, createdResponse)
			assertKeys(t, created, []string{"id", "name", "schedule", "command", "timeout_seconds", "max_retries", "backoff", "status", "next_run_at", "created_at", "updated_at"})
			fetched := decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
			if !reflect.DeepEqual(fetched, job) {
				t.Fatalf("%s created/get job mismatch", backend)
			}
			pauseResponse := request(t, handler, "POST", "/api/jobs/"+job.ID+"/pause", `{}`, 200)
			paused := decode[jobDTO](t, pauseResponse)
			if paused.Status != "paused" || !reflect.DeepEqual(paused.NextRunAt, job.NextRunAt) {
				t.Fatalf("%s pause changed next_run_at", backend)
			}
			jobs := decode[[]jobDTO](t, request(t, handler, "GET", "/api/jobs?status=paused", "", 200))
			if len(jobs) != 1 || !reflect.DeepEqual(jobs[0], paused) {
				t.Fatalf("%s job list=%+v", backend, jobs)
			}
			runResponse := request(t, handler, "POST", "/api/jobs/"+job.ID+"/run", `{}`, 201)
			run := decode[runDTO](t, runResponse)
			if run.JobID != job.ID || run.Status != "pending" || run.Attempt != 1 {
				t.Fatalf("%s manual run=%+v", backend, run)
			}
			fetchedRun := decode[runDTO](t, request(t, handler, "GET", "/api/runs/"+run.ID, "", 200))
			if !reflect.DeepEqual(fetchedRun, run) {
				t.Fatalf("%s created/get run mismatch", backend)
			}
			fetched = decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
			if !reflect.DeepEqual(fetched, paused) {
				t.Fatalf("%s manual run changed job", backend)
			}
			at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
			for _, log := range []store.Log{
				{RunID: run.ID, Stream: "stderr", Chunk: "second\n", Seq: 2, CreatedAt: at},
				{RunID: run.ID, Stream: "stdout", Chunk: "first\n", Seq: 1, CreatedAt: at},
			} {
				if err := st.AppendLog(context.Background(), log); err != nil {
					t.Fatal(err)
				}
			}
			logsResponse := request(t, handler, "GET", "/api/runs/"+run.ID+"/logs", "", 200)
			logs := decode[[]logDTO](t, logsResponse)
			if len(logs) != 2 || logs[0].Seq != 1 || logs[1].Seq != 2 || logs[0].RunID != run.ID || logs[1].RunID != run.ID || logs[0].Chunk != "first\n" || logs[1].Chunk != "second\n" {
				t.Fatalf("%s logs=%+v", backend, logs)
			}
			logMaps := decode[[]map[string]any](t, logsResponse)
			for _, log := range logMaps {
				assertKeys(t, log, []string{"run_id", "stream", "chunk", "seq", "created_at"})
				delete(log, "run_id")
			}
			pausedMap := decode[map[string]any](t, pauseResponse)
			for _, value := range []map[string]any{created, pausedMap} {
				delete(value, "id")
				delete(value, "created_at")
				delete(value, "updated_at")
			}
			runMap := decode[map[string]any](t, runResponse)
			assertKeys(t, runMap, []string{"id", "job_id", "status", "attempt", "scheduled_at", "started_at", "finished_at", "exit_code", "duration_ms", "error", "created_at"})
			for _, key := range []string{"id", "job_id", "scheduled_at", "created_at"} {
				delete(runMap, key)
			}
			status := decode[map[string]any](t, request(t, handler, "GET", "/api/status", "", 200))
			snapshot := []map[string]any{created, pausedMap, runMap, logMaps[0], logMaps[1], status}
			if baseline == nil {
				baseline = snapshot
			} else if !reflect.DeepEqual(snapshot, baseline) {
				t.Fatalf("Redis/SQLite response mismatch: got=%+v want=%+v", snapshot, baseline)
			}
		})
	}
}
