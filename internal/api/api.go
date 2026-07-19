package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/cron"
	"github.com/AqViolet/aqs-schedular/internal/store"
)

const maxBodyBytes = 64 * 1024

var jobStates = []string{"active", "paused", "dead"}
var runStates = []string{"pending", "running", "succeeded", "failed", "dead", "missed"}

type api struct {
	store  store.Store
	logger *slog.Logger
}

func New(st store.Store, token string, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	a := &api{store: st, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{Status: "ok"})
	})
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("POST /api/jobs", a.createJob)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("PATCH /api/jobs/{id}", a.patchJob)
	mux.HandleFunc("DELETE /api/jobs/{id}", action(a.deleteJob))
	mux.HandleFunc("POST /api/jobs/{id}/pause", action(a.setJobStatus("paused")))
	mux.HandleFunc("POST /api/jobs/{id}/resume", action(a.setJobStatus("active")))
	mux.HandleFunc("POST /api/jobs/{id}/run", action(a.runJob))
	mux.HandleFunc("GET /api/jobs/{id}/runs", a.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", a.getRun)
	mux.HandleFunc("GET /api/runs/{id}/logs", a.listLogs)
	mux.HandleFunc("POST /api/runs/{id}/retry", action(a.retryRun))
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	expected := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
			auth := r.Header.Values("Authorization")
			var provided string
			valid := len(auth) == 1
			if valid {
				scheme, value, ok := strings.Cut(auth[0], " ")
				valid = ok && strings.EqualFold(scheme, "Bearer") && value != ""
				provided = value
			}
			actual := sha256.Sum256([]byte(provided))
			matches := subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
			if !valid || strings.TrimSpace(token) == "" || !matches {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func action(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		decoder := json.NewDecoder(r.Body)
		var fields map[string]json.RawMessage
		err := decoder.Decode(&fields)
		if err == io.EOF {
			handler(w, r)
			return
		}
		if err != nil || fields == nil || len(fields) != 0 {
			writeError(w, http.StatusBadRequest, "body must be empty or an empty JSON object no larger than 64 KiB")
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, http.StatusBadRequest, "body must contain exactly one JSON object")
			return
		}
		handler(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, struct {
		Error string `json:"error"`
	}{Error: message})
}

func (a *api) storeError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	a.logger.ErrorContext(r.Context(), "management API store error", "method", r.Method, "path", r.URL.Path, "error", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func listStatus(w http.ResponseWriter, r *http.Request, states []string) (string, bool) {
	values, ok := r.URL.Query()["status"]
	if !ok {
		return "", true
	}
	if len(values) == 1 {
		if values[0] == "" {
			return "", true
		}
		for _, state := range states {
			if values[0] == state {
				return state, true
			}
		}
	}
	writeError(w, http.StatusBadRequest, "invalid status filter")
	return "", false
}

func (a *api) listJobs(w http.ResponseWriter, r *http.Request) {
	status, ok := listStatus(w, r, jobStates)
	if !ok {
		return
	}
	jobs, err := a.store.ListJobs(r.Context(), status)
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	out := make([]jobDTO, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, jobResponse(job))
	}
	writeJSON(w, http.StatusOK, out)
}

type jobInput struct {
	Name           *string `json:"name"`
	Schedule       *string `json:"schedule"`
	Command        *string `json:"command"`
	TimeoutSeconds *int    `json:"timeout_seconds"`
	MaxRetries     *int    `json:"max_retries"`
	Backoff        *string `json:"backoff"`
}

func decodeJob(w http.ResponseWriter, r *http.Request) (jobInput, bool) {
	var input jobInput
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON object no larger than 64 KiB")
		return input, false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "body must contain exactly one JSON object")
		return input, false
	}
	for name, value := range fields {
		var target any
		switch name {
		case "name":
			target = &input.Name
		case "schedule":
			target = &input.Schedule
		case "command":
			target = &input.Command
		case "timeout_seconds":
			target = &input.TimeoutSeconds
		case "max_retries":
			target = &input.MaxRetries
		case "backoff":
			target = &input.Backoff
		default:
			writeError(w, http.StatusBadRequest, "unknown field: "+name)
			return input, false
		}
		if string(value) == "null" || json.Unmarshal(value, target) != nil {
			writeError(w, http.StatusBadRequest, "invalid field: "+name)
			return input, false
		}
	}
	return input, true
}

func applyInput(job *store.Job, input jobInput) {
	if input.Name != nil {
		job.Name = *input.Name
	}
	if input.Schedule != nil {
		job.Schedule = strings.TrimSpace(*input.Schedule)
	}
	if input.Command != nil {
		job.Command = *input.Command
	}
	if input.TimeoutSeconds != nil {
		job.TimeoutSeconds = *input.TimeoutSeconds
	}
	if input.MaxRetries != nil {
		job.MaxRetries = *input.MaxRetries
	}
	if input.Backoff != nil {
		job.Backoff = *input.Backoff
	}
}

func validateJob(job store.Job) (cron.Schedule, error) {
	if strings.TrimSpace(job.Name) == "" || strings.TrimSpace(job.Command) == "" || strings.TrimSpace(job.Schedule) == "" {
		return nil, errors.New("name, schedule, and command are required nonempty strings")
	}
	if job.TimeoutSeconds < 0 || int64(job.TimeoutSeconds) > int64((1<<63-1)/time.Second) {
		return nil, errors.New("timeout_seconds must be nonnegative and fit a time.Duration")
	}
	if job.MaxRetries < 0 {
		return nil, errors.New("max_retries must be nonnegative")
	}
	if job.Backoff != "exponential" && job.Backoff != "linear" && job.Backoff != "none" {
		return nil, errors.New("backoff must be exponential, linear, or none")
	}
	schedule, err := cron.Parse(job.Schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule: %w", err)
	}
	return schedule, nil
}

func nextRun(schedule cron.Schedule, now time.Time) *time.Time {
	next := schedule.NextAfter(now)
	if once, ok := schedule.(cron.Once); ok && next.IsZero() {
		next = once.At()
	}
	if next.IsZero() {
		return nil
	}
	next = next.UTC().Truncate(time.Second)
	return &next
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (a *api) createJob(w http.ResponseWriter, r *http.Request) {
	input, ok := decodeJob(w, r)
	if !ok {
		return
	}
	job := store.Job{Status: "active", Backoff: "exponential"}
	applyInput(&job, input)
	schedule, err := validateJob(job)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	job.ID, err = newID()
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	job.CreatedAt = time.Now().UTC().Truncate(time.Second)
	job.UpdatedAt = job.CreatedAt
	job.NextRunAt = nextRun(schedule, job.CreatedAt)
	if err := a.store.CreateJob(r.Context(), job); err != nil {
		a.storeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/jobs/"+job.ID)
	writeJSON(w, http.StatusCreated, jobResponse(job))
}

func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, jobResponse(job))
}

func (a *api) patchJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	input, ok := decodeJob(w, r)
	if !ok {
		return
	}
	oldSchedule := job.Schedule
	applyInput(&job, input)
	schedule, err := validateJob(job)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	scheduleChanged := job.Schedule != oldSchedule
	if scheduleChanged {
		job.NextRunAt = nextRun(schedule, time.Now())
	}
	if configuration, ok := a.store.(store.JobConfigurationStore); ok {
		err = configuration.UpdateJobConfiguration(r.Context(), job, scheduleChanged)
	} else {
		err = a.store.UpdateJob(r.Context(), job)
	}
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	a.getJob(w, r)
}

func (a *api) deleteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteJob(r.Context(), r.PathValue("id")); err != nil {
		a.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) setJobStatus(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.store.SetJobStatus(r.Context(), r.PathValue("id"), status); err != nil {
			a.storeError(w, r, err)
			return
		}
		a.getJob(w, r)
	}
}

func (a *api) runJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	a.enqueue(w, r, job.ID, 1)
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request, jobID string, attempt int) {
	id, err := newID()
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	now := time.Now().UTC().Truncate(time.Second)
	run := store.Run{ID: id, JobID: jobID, Status: "pending", Attempt: attempt, ScheduledAt: now, CreatedAt: now}
	if err := a.store.CreateRun(r.Context(), run); err != nil {
		a.storeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/runs/"+run.ID)
	writeJSON(w, http.StatusCreated, runResponse(run))
}

func (a *api) listRuns(w http.ResponseWriter, r *http.Request) {
	status, ok := listStatus(w, r, runStates)
	if !ok {
		return
	}
	jobID := r.PathValue("id")
	if _, err := a.store.GetJob(r.Context(), jobID); err != nil {
		a.storeError(w, r, err)
		return
	}
	runs, err := a.store.ListRuns(r.Context(), jobID)
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	out := make([]runDTO, 0, len(runs))
	for _, run := range runs {
		if run.JobID == jobID && (status == "" || run.Status == status) {
			out = append(out, runResponse(run))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := a.store.GetRun(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runResponse(run))
}

func (a *api) listLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, err := a.store.GetRun(r.Context(), runID); err != nil {
		a.storeError(w, r, err)
		return
	}
	logs, err := a.store.ListLogs(r.Context(), runID)
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	out := make([]logDTO, 0, len(logs))
	for _, log := range logs {
		if log.RunID == runID {
			out = append(out, logDTO{RunID: log.RunID, Stream: log.Stream, Chunk: log.Chunk, Seq: log.Seq, CreatedAt: timestamp(log.CreatedAt)})
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) retryRun(w http.ResponseWriter, r *http.Request) {
	run, err := a.store.GetRun(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	if run.Status != "dead" && run.Status != "failed" {
		writeError(w, http.StatusConflict, "only dead or failed runs can be retried")
		return
	}
	if run.Attempt < 1 || run.Attempt == int(^uint(0)>>1) {
		writeError(w, http.StatusConflict, "invalid retry attempt")
		return
	}
	if _, err := a.store.GetJob(r.Context(), run.JobID); err != nil {
		a.storeError(w, r, err)
		return
	}
	a.enqueue(w, r, run.JobID, run.Attempt+1)
}

func counts(states []string) map[string]int {
	out := make(map[string]int, len(states))
	for _, state := range states {
		out[state] = 0
	}
	return out
}

func (a *api) status(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context(), "")
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	out := struct {
		Jobs map[string]int `json:"jobs"`
		Runs map[string]int `json:"runs"`
	}{Jobs: counts(jobStates), Runs: counts(runStates)}
	for _, job := range jobs {
		out.Jobs[job.Status]++
		runs, err := a.store.ListRuns(r.Context(), job.ID)
		if err != nil {
			a.storeError(w, r, err)
			return
		}
		for _, run := range runs {
			if run.JobID == job.ID {
				out.Runs[run.Status]++
			}
		}
	}
	writeJSON(w, http.StatusOK, out)
}
