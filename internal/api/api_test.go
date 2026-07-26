package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

const testToken = "management-secret"
const validJob = `{"name":"example","schedule":"@every 1m","command":"echo hello"}`

func setup(t *testing.T) (*store.SQLite, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, New(st, testToken, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func request(t *testing.T, handler http.Handler, method, path, body string, code int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != code {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, w.Code, code, w.Body.String())
	}
	if code != http.StatusNoContent && w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type=%q", w.Header().Get("Content-Type"))
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unexpected CORS header")
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("cache control=%q", w.Header().Get("Cache-Control"))
	}
	return w
}

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func create(t *testing.T, handler http.Handler) jobDTO {
	t.Helper()
	w := request(t, handler, "POST", "/api/jobs", validJob, 201)
	job := decode[jobDTO](t, w)
	if w.Header().Get("Location") != "/api/jobs/"+job.ID {
		t.Fatal("missing job Location")
	}
	return job
}

func TestAuthentication(t *testing.T) {
	st, handler := setup(t)
	for _, path := range []string{"/api/jobs", "/api/jobs/missing", "/api/status", "/api/unknown", "/api"} {
		for _, header := range []string{"", "Bearer wrong", "Basic " + testToken, "Bearer", "Bearer " + testToken + " ", "Bearer  " + testToken} {
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", header)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") != "Bearer" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("path=%s header=%q status=%d", path, header, w.Code)
			}
		}
	}
	for _, token := range []string{"", "   "} {
		h := New(st, token, nil)
		r := httptest.NewRequest("GET", "/api/jobs", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("empty configured token: status=%d", w.Code)
		}
	}
	r := httptest.NewRequest("GET", "/api/jobs", nil)
	r.Header.Add("Authorization", "Bearer "+testToken)
	r.Header.Add("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("accepted multiple Authorization headers")
	}
	r = httptest.NewRequest("GET", "/healthz", nil)
	w = httptest.NewRecorder()
	New(st, "", nil).ServeHTTP(w, r)
	if w.Code != 200 || w.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("health=%d %s", w.Code, w.Body.String())
	}
	request(t, handler, "GET", "/api/jobs", "", 200)
	for _, method := range []string{"POST", "PATCH", "DELETE"} {
		r := httptest.NewRequest(method, "/api/jobs/missing", strings.NewReader(validJob))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("unauthenticated mutation %s: %d", method, w.Code)
		}
	}
	jobs, err := st.ListJobs(context.Background(), "")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("auth mutated jobs: %v %v", jobs, err)
	}
}

func TestJobCRUDAndPatchPreservesState(t *testing.T) {
	st, handler := setup(t)
	if got := request(t, handler, "GET", "/api/jobs", "", 200).Body.String(); got != "[]\n" {
		t.Fatalf("empty jobs=%s", got)
	}
	job := create(t, handler)
	if job.ID == "" || job.Status != "active" || job.Backoff != "exponential" || job.TimeoutSeconds != 0 || job.MaxRetries != 0 || job.NextRunAt == nil {
		t.Fatalf("job defaults=%+v", job)
	}
	stored, err := st.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
	if !reflect.DeepEqual(got, job) {
		t.Fatalf("created vs stored DTO: %+v %+v", job, got)
	}
	paused := decode[jobDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/pause", "", 200))
	if paused.Status != "paused" || !reflect.DeepEqual(paused.NextRunAt, job.NextRunAt) {
		t.Fatalf("pause changed schedule: %+v", paused)
	}
	patched := decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"name":"renamed","timeout_seconds":5,"max_retries":3,"backoff":"linear"}`, 200))
	if patched.Name != "renamed" || patched.TimeoutSeconds != 5 || patched.MaxRetries != 3 || patched.Backoff != "linear" || patched.Command != job.Command || patched.Schedule != job.Schedule || patched.Status != "paused" || !reflect.DeepEqual(patched.NextRunAt, job.NextRunAt) || !reflect.DeepEqual(patched.CreatedAt, job.CreatedAt) {
		t.Fatalf("patch=%+v", patched)
	}
	patched = decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"timeout_seconds":0,"max_retries":0,"command":"echo replaced"}`, 200))
	if patched.TimeoutSeconds != 0 || patched.MaxRetries != 0 || patched.Command != "echo replaced" {
		t.Fatalf("zero values not applied: %+v", patched)
	}
	patched = decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"schedule":"@every 1m"}`, 200))
	if !reflect.DeepEqual(patched.NextRunAt, job.NextRunAt) {
		t.Fatal("same schedule reset next_run_at")
	}
	once := "2099-01-02T03:04:05Z"
	patched = decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"schedule":"once:`+once+`"}`, 200))
	if patched.Status != "paused" || patched.NextRunAt == nil || *patched.NextRunAt != once {
		t.Fatalf("schedule patch=%+v", patched)
	}
	resumed := decode[jobDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/resume", "", 200))
	if resumed.Status != "active" || !reflect.DeepEqual(resumed.NextRunAt, patched.NextRunAt) {
		t.Fatalf("resume=%+v", resumed)
	}
	jobs := decode[[]jobDTO](t, request(t, handler, "GET", "/api/jobs?status=active", "", 200))
	if len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("jobs=%+v", jobs)
	}
	request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"status":"dead"}`, 400)
	request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"next_run_at":null}`, 400)
	request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"id":"other"}`, 400)
	if stored.CreatedAt.UTC().Format(time.RFC3339) != *job.CreatedAt {
		t.Fatal("creation timestamp mismatch")
	}
	request(t, handler, "DELETE", "/api/jobs/"+job.ID, "", 204)
	request(t, handler, "GET", "/api/jobs/"+job.ID, "", 404)
	request(t, handler, "DELETE", "/api/jobs/"+job.ID, "", 404)
}

func TestValidation(t *testing.T) {
	st, handler := setup(t)
	invalid := []string{
		``, `null`, `[]`, `{`, `{}`, `{"name":"n","schedule":"@daily"}`,
		`{"name":" ","schedule":"@daily","command":"echo"}`,
		`{"name":"n","schedule":" ","command":"echo"}`,
		`{"name":"n","schedule":"@daily","command":" "}`,
		`{"name":"n","schedule":"invalid","command":"echo"}`,
		`{"name":"n","schedule":"once:garbage","command":"echo"}`,
		`{"name":"n","schedule":"@daily","command":"echo","timeout_seconds":-1}`,
		`{"name":"n","schedule":"@daily","command":"echo","timeout_seconds":9223372037}`,
		`{"name":"n","schedule":"@daily","command":"echo","max_retries":-1}`,
		`{"name":"n","schedule":"@daily","command":"echo","backoff":"random"}`,
		`{"name":"n","schedule":"@daily","command":"echo","backoff":""}`,
		`{"name":"n","schedule":"@daily","command":"echo","timeout_seconds":1.5}`,
		`{"name":"n","schedule":"@daily","command":"echo","max_retries":"3"}`,
		`{"name":"n","schedule":"@daily","command":"echo","surprise":true}`,
		`{"Name":"n","schedule":"@daily","command":"echo"}`,
		validJob + `{}`, validJob + ` null`, validJob + ` trailing`,
		`{"name":null,"schedule":"@daily","command":"echo"}`,
		`{"name":"n","schedule":"@daily","command":"echo","timeout_seconds":null}`,
		`{"name":"` + strings.Repeat("x", maxBodyBytes) + `","schedule":"@daily","command":"echo"}`,
		validJob + strings.Repeat(" ", maxBodyBytes),
	}
	for _, body := range invalid {
		request(t, handler, "POST", "/api/jobs", body, 400)
	}
	jobs, err := st.ListJobs(context.Background(), "")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("invalid body created job: %v %v", jobs, err)
	}
	job := create(t, handler)
	for _, body := range []string{`null`, `{"command":null}`, `{"max_retries":null}`, `{"name":""}`, `{"schedule":"bad"}`, `{"timeout_seconds":-1}`, `{"backoff":"bad"}`, `{} {}`, `{"bogus":1}`} {
		request(t, handler, "PATCH", "/api/jobs/"+job.ID, body, 400)
	}
	got := decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
	if !reflect.DeepEqual(got, job) {
		t.Fatalf("invalid patch changed job: %+v %+v", got, job)
	}
	for _, path := range []string{"/api/jobs?status=running", "/api/jobs?status=ACTIVE", "/api/jobs?status=active&status=paused", "/api/jobs/" + job.ID + "/runs?status=active", "/api/jobs/" + job.ID + "/runs?status=dead&status=failed"} {
		request(t, handler, "GET", path, "", 400)
	}
	for _, schedule := range []string{"once:2099-01-02T03:04:05+02:00", "once:2000-01-02T03:04:05Z", "0 * * * *"} {
		body := `{"name":"once","schedule":"` + schedule + `","command":"echo"}`
		created := decode[jobDTO](t, request(t, handler, "POST", "/api/jobs", body, 201))
		if created.NextRunAt == nil {
			t.Fatalf("next_run_at absent for %s", schedule)
		}
		if strings.HasPrefix(schedule, "once:") {
			at, _ := time.Parse(time.RFC3339, strings.TrimPrefix(schedule, "once:"))
			if *created.NextRunAt != at.UTC().Format(time.RFC3339) {
				t.Fatalf("once next=%s", *created.NextRunAt)
			}
		}
	}
}

func TestRunsLogsRetriesAndOwnership(t *testing.T) {
	st, handler := setup(t)
	ctx := context.Background()
	first, second := create(t, handler), create(t, handler)
	w := request(t, handler, "POST", "/api/jobs/"+first.ID+"/run", "", 201)
	run := decode[runDTO](t, w)
	if run.Status != "pending" || run.Attempt != 1 || run.JobID != first.ID || run.ID == "" || w.Header().Get("Location") != "/api/runs/"+run.ID {
		t.Fatalf("manual run=%+v", run)
	}
	if run.StartedAt != nil || run.FinishedAt != nil || run.ExitCode != nil || run.DurationMS != nil {
		t.Fatalf("pending result fields=%+v", run)
	}
	job := decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+first.ID, "", 200))
	if !reflect.DeepEqual(first, job) {
		t.Fatal("manual run changed job")
	}
	other := decode[runDTO](t, request(t, handler, "POST", "/api/jobs/"+second.ID+"/run", "", 201))
	for _, pair := range []struct{ job, run string }{{first.ID, run.ID}, {second.ID, other.ID}} {
		runs := decode[[]runDTO](t, request(t, handler, "GET", "/api/jobs/"+pair.job+"/runs", "", 200))
		if len(runs) != 1 || runs[0].ID != pair.run || runs[0].JobID != pair.job {
			t.Fatalf("runs ownership=%+v", runs)
		}
	}
	if got := request(t, handler, "GET", "/api/runs/"+run.ID+"/logs", "", 200).Body.String(); got != "[]\n" {
		t.Fatalf("empty logs=%s", got)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, log := range []store.Log{
		{RunID: run.ID, Stream: "stderr", Chunk: "second\n", Seq: 2, CreatedAt: now},
		{RunID: run.ID, Stream: "stdout", Chunk: "first\n", Seq: 1, CreatedAt: now},
		{RunID: other.ID, Stream: "stdout", Chunk: "other job", Seq: 1, CreatedAt: now},
	} {
		if err := st.AppendLog(ctx, log); err != nil {
			t.Fatal(err)
		}
	}
	logResponse := request(t, handler, "GET", "/api/runs/"+run.ID+"/logs", "", 200)
	logs := decode[[]logDTO](t, logResponse)
	logMaps := decode[[]map[string]any](t, logResponse)
	assertKeys(t, logMaps[0], []string{"run_id", "stream", "chunk", "seq", "created_at"})
	if logs[0].CreatedAt == nil || *logs[0].CreatedAt != now.Format(time.RFC3339) {
		t.Fatalf("log timestamp=%+v", logs[0])
	}
	if len(logs) != 2 || logs[0].Seq != 1 || logs[0].Chunk != "first\n" || logs[1].Seq != 2 || logs[1].Stream != "stderr" || logs[0].RunID != run.ID || logs[1].RunID != run.ID {
		t.Fatalf("logs=%+v", logs)
	}
	for _, state := range []string{"pending", "running", "succeeded", "missed"} {
		saved, err := st.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		saved.Status = state
		if err := st.UpdateRun(ctx, saved); err != nil {
			t.Fatal(err)
		}
		request(t, handler, "POST", "/api/runs/"+run.ID+"/retry", "", 409)
	}
	for _, state := range []string{"dead", "failed"} {
		saved, err := st.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		exit, duration := 7, int64(1000)
		saved.Status, saved.Attempt, saved.Error = state, 3, "failure"
		saved.StartedAt, saved.FinishedAt, saved.ExitCode, saved.DurationMS = &now, &now, &exit, &duration
		if err := st.UpdateRun(ctx, saved); err != nil {
			t.Fatal(err)
		}
		retry := decode[runDTO](t, request(t, handler, "POST", "/api/runs/"+run.ID+"/retry", "", 201))
		if retry.ID == run.ID || retry.JobID != first.ID || retry.Attempt != 4 || retry.Status != "pending" || retry.Error != "" || retry.ExitCode != nil || retry.StartedAt != nil || retry.FinishedAt != nil || retry.DurationMS != nil {
			t.Fatalf("retry=%+v", retry)
		}
		original := decode[runDTO](t, request(t, handler, "GET", "/api/runs/"+run.ID, "", 200))
		if original.Status != state || original.Attempt != 3 || original.Error != "failure" || original.ExitCode == nil || *original.ExitCode != 7 {
			t.Fatalf("retry changed original=%+v", original)
		}
	}
	filtered := decode[[]runDTO](t, request(t, handler, "GET", "/api/jobs/"+first.ID+"/runs?status=failed", "", 200))
	if len(filtered) != 1 || filtered[0].ID != run.ID {
		t.Fatalf("filtered=%+v", filtered)
	}
	request(t, handler, "DELETE", "/api/jobs/"+first.ID, "", 204)
	request(t, handler, "GET", "/api/runs/"+run.ID, "", 404)
	request(t, handler, "GET", "/api/runs/"+run.ID+"/logs", "", 404)
	request(t, handler, "GET", "/api/runs/"+other.ID, "", 200)
}

func TestStatusAndNotFound(t *testing.T) {
	st, handler := setup(t)
	type statusDTO struct {
		Jobs map[string]int `json:"jobs"`
		Runs map[string]int `json:"runs"`
	}
	empty := decode[statusDTO](t, request(t, handler, "GET", "/api/status", "", 200))
	if !reflect.DeepEqual(empty.Jobs, counts(jobStates)) || !reflect.DeepEqual(empty.Runs, counts(runStates)) {
		t.Fatalf("empty status=%+v", empty)
	}
	for i, state := range jobStates {
		job := create(t, handler)
		if err := st.SetJobStatus(context.Background(), job.ID, state); err != nil {
			t.Fatal(err)
		}
		for _, runState := range runStates {
			run := decode[runDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/run", "", 201))
			saved, err := st.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			saved.Status = runState
			if err := st.UpdateRun(context.Background(), saved); err != nil {
				t.Fatal(err)
			}
		}
		status := decode[statusDTO](t, request(t, handler, "GET", "/api/status", "", 200))
		if status.Jobs[state] != 1 {
			t.Fatalf("job state count=%+v", status)
		}
		for _, runState := range runStates {
			if status.Runs[runState] != i+1 {
				t.Fatalf("run counts=%+v", status)
			}
		}
	}
	for _, endpoint := range []struct{ method, path string }{
		{"GET", "/api/jobs/missing"}, {"PATCH", "/api/jobs/missing"}, {"DELETE", "/api/jobs/missing"},
		{"POST", "/api/jobs/missing/pause"}, {"POST", "/api/jobs/missing/resume"}, {"POST", "/api/jobs/missing/run"},
		{"GET", "/api/jobs/missing/runs"}, {"GET", "/api/runs/missing"}, {"GET", "/api/runs/missing/logs"},
		{"POST", "/api/runs/missing/retry"}, {"GET", "/api/unknown"},
	} {
		request(t, handler, endpoint.method, endpoint.path, `{}`, 404)
	}
}

func TestActionBodiesAndAbsentNextRun(t *testing.T) {
	st, handler := setup(t)
	job := create(t, handler)
	run := decode[runDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/run", `{}`, 201))
	for _, endpoint := range []struct{ method, path string }{
		{"POST", "/api/jobs/" + job.ID + "/pause"},
		{"POST", "/api/jobs/" + job.ID + "/resume"},
		{"POST", "/api/jobs/" + job.ID + "/run"},
		{"POST", "/api/runs/" + run.ID + "/retry"},
		{"DELETE", "/api/jobs/" + job.ID},
	} {
		for _, body := range []string{`{"id":"other"}`, `{} {}`, `null`, `[]`, strings.Repeat(" ", maxBodyBytes+1)} {
			request(t, handler, endpoint.method, endpoint.path, body, 400)
		}
	}
	saved, err := st.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved.Status, saved.NextRunAt = "dead", nil
	if err := st.UpdateJob(context.Background(), saved); err != nil {
		t.Fatal(err)
	}
	patched := decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"name":"renamed"}`, 200))
	if patched.Status != "dead" || patched.NextRunAt != nil {
		t.Fatalf("patch resurrected completed job: %+v", patched)
	}
	resumed := decode[jobDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/resume", `{}`, 200))
	if resumed.Status != "active" || resumed.NextRunAt != nil {
		t.Fatalf("resume changed next_run_at: %+v", resumed)
	}
}

func TestResponseContract(t *testing.T) {
	_, handler := setup(t)
	job := create(t, handler)
	jobMap := decode[map[string]any](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
	assertKeys(t, jobMap, []string{"id", "name", "schedule", "command", "timeout_seconds", "max_retries", "backoff", "status", "next_run_at", "created_at", "updated_at"})
	run := decode[runDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/run", "", 201))
	runMap := decode[map[string]any](t, request(t, handler, "GET", "/api/runs/"+run.ID, "", 200))
	assertKeys(t, runMap, []string{"id", "job_id", "status", "attempt", "scheduled_at", "started_at", "finished_at", "exit_code", "duration_ms", "error", "created_at"})
	for _, key := range []string{"started_at", "finished_at", "exit_code", "duration_ms"} {
		if runMap[key] != nil {
			t.Fatalf("%s must be null", key)
		}
	}
	for _, value := range []any{jobMap["created_at"], jobMap["updated_at"], jobMap["next_run_at"], runMap["scheduled_at"], runMap["created_at"]} {
		if _, err := time.Parse(time.RFC3339, value.(string)); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(jobResponse(store.Job{}))
	if err != nil {
		t.Fatal(err)
	}
	var zero map[string]any
	if err := json.Unmarshal(encoded, &zero); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"created_at", "updated_at", "next_run_at"} {
		if zero[key] != nil {
			t.Fatalf("zero timestamp %s not null", key)
		}
	}
}

func assertKeys(t *testing.T, value map[string]any, keys []string) {
	t.Helper()
	if len(value) != len(keys) {
		t.Fatalf("keys=%v want=%v", value, keys)
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			t.Fatalf("missing key=%s", key)
		}
	}
}

type failingStore struct {
	store.Store
	failRuns bool
}

func (s failingStore) ListJobs(ctx context.Context, status string) ([]store.Job, error) {
	if s.failRuns {
		return s.Store.ListJobs(ctx, status)
	}
	return nil, errors.New("private database detail")
}

func (s failingStore) ListRuns(context.Context, string) ([]store.Run, error) {
	return nil, errors.New("private database detail")
}

func TestStoreErrors(t *testing.T) {
	st, handler := setup(t)
	job := create(t, handler)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, failRuns := range []bool{false, true} {
		h := New(failingStore{Store: st, failRuns: failRuns}, testToken, logger)
		w := request(t, h, "GET", "/api/status", "", 500)
		if w.Body.String() != "{\"error\":\"internal server error\"}\n" {
			t.Fatalf("store error leaked: %s", w.Body.String())
		}
		if failRuns {
			request(t, h, "GET", "/api/jobs/"+job.ID+"/runs", "", 500)
		} else {
			request(t, h, "GET", "/api/jobs", "", 500)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []struct{ method, path, body string }{
		{"GET", "/api/jobs/" + job.ID, ""}, {"POST", "/api/jobs", validJob},
		{"DELETE", "/api/jobs/" + job.ID, ""}, {"POST", "/api/jobs/" + job.ID + "/pause", ""},
		{"GET", "/api/runs/missing", ""}, {"GET", "/api/runs/missing/logs", ""},
	} {
		request(t, handler, endpoint.method, endpoint.path, endpoint.body, 500)
	}
}

func TestPaginationAndQueryValidation(t *testing.T) {
	st, handler := setup(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	job := store.Job{ID: "page-job", Name: "page", Schedule: "@daily", Command: "echo ok", Status: "active", CreatedAt: now, UpdatedAt: now}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		run := store.Run{ID: "run-" + string(rune('0'+i)), JobID: job.ID, Status: "succeeded", Attempt: 1, ScheduledAt: now.Add(time.Duration(i) * time.Minute), CreatedAt: now.Add(time.Duration(i) * time.Minute)}
		if err := st.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		for seq := 1; seq <= 12; seq++ {
			if run.ID == "run-5" {
				if err := st.AppendLog(ctx, store.Log{RunID: run.ID, Stream: "stdout", Chunk: string(rune('a' + seq - 1)), Seq: int64(seq), CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	runs := decode[[]runDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID+"/runs?limit=2", "", 200))
	if len(runs) != 2 || runs[0].ID != "run-5" || runs[1].ID != "run-4" {
		t.Fatalf("first page=%+v", runs)
	}
	runs = decode[[]runDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID+"/runs?limit=2&offset=2", "", 200))
	if len(runs) != 2 || runs[0].ID != "run-3" || runs[1].ID != "run-2" {
		t.Fatalf("second page=%+v", runs)
	}
	jobs := decode[[]jobDTO](t, request(t, handler, "GET", "/api/jobs?limit=1", "", 200))
	if len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("job page=%+v", jobs)
	}
	logs := decode[[]logDTO](t, request(t, handler, "GET", "/api/runs/run-5/logs?limit=3", "", 200))
	if len(logs) != 3 || logs[0].Seq != 10 || logs[2].Seq != 12 {
		t.Fatalf("tail logs=%+v", logs)
	}
	logs = decode[[]logDTO](t, request(t, handler, "GET", "/api/runs/run-5/logs?limit=3&before=11", "", 200))
	if len(logs) != 3 || logs[0].Seq != 8 || logs[2].Seq != 10 {
		t.Fatalf("before logs=%+v", logs)
	}
	logs = decode[[]logDTO](t, request(t, handler, "GET", "/api/runs/run-5/logs?limit=2&after=9", "", 200))
	if len(logs) != 2 || logs[0].Seq != 10 || logs[1].Seq != 11 {
		t.Fatalf("after logs=%+v", logs)
	}
	for _, path := range []string{
		"/api/jobs?limit=0", "/api/jobs?limit=1001", "/api/jobs?offset=-1",
		"/api/jobs?limit=1&limit=2", "/api/jobs?status=active&status=paused",
		"/api/jobs?unknown=1", "/api/status?limit=1",
		"/api/jobs/" + job.ID + "/runs?limit=501",
		"/api/runs/run-5/logs?limit=0", "/api/runs/run-5/logs?before=5&after=2",
		"/api/runs/run-5/logs?before=-1",
	} {
		request(t, handler, "GET", path, "", 400)
	}
}
