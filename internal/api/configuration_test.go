package api

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

type interleavingStore struct {
	store.Store
	configuration   store.JobConfigurationStore
	read            chan struct{}
	proceed         chan struct{}
	once            sync.Once
	scheduleChanged bool
	calls           int
}

func (s *interleavingStore) GetJob(ctx context.Context, id string) (store.Job, error) {
	job, err := s.Store.GetJob(ctx, id)
	if err != nil {
		return job, err
	}
	s.once.Do(func() {
		close(s.read)
		select {
		case <-s.proceed:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return job, err
}

func (s *interleavingStore) UpdateJob(context.Context, store.Job) error {
	return errors.New("full job updates must not be used")
}

func (s *interleavingStore) UpdateJobConfiguration(ctx context.Context, job store.Job, changed bool) error {
	s.calls++
	s.scheduleChanged = changed
	return s.configuration.UpdateJobConfiguration(ctx, job, changed)
}

func TestPatchPreservesConcurrentSchedulerState(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				body    string
				changed bool
				nilNext bool
			}{
				{name: "configuration", body: `{"name":"renamed","command":"echo replaced","timeout_seconds":5,"max_retries":2,"backoff":"none"}`},
				{name: "same schedule", body: `{"name":"renamed","schedule":"@every 1m"}`},
				{name: "changed schedule", body: `{"name":"renamed","schedule":"once:2099-01-02T03:04:05Z"}`, changed: true},
				{name: "completed schedule", body: `{"name":"renamed"}`, nilNext: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					st, handler := setupBackend(t, backend)
					configuration, ok := st.(store.JobConfigurationStore)
					if !ok {
						t.Fatalf("%s does not support configuration updates", backend)
					}
					job := create(t, handler)
					stored, err := st.GetJob(context.Background(), job.ID)
					if err != nil {
						t.Fatal(err)
					}
					wrapped := &interleavingStore{Store: st, configuration: configuration, read: make(chan struct{}), proceed: make(chan struct{})}
					handler = New(wrapped, testToken, slog.New(slog.NewTextHandler(io.Discard, nil)))
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					r := httptest.NewRequest("PATCH", "/api/jobs/"+job.ID, strings.NewReader(test.body)).WithContext(ctx)
					r.Header.Set("Authorization", "Bearer "+testToken)
					done := make(chan *httptest.ResponseRecorder, 1)
					go func() {
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						done <- w
					}()
					select {
					case <-wrapped.read:
					case <-ctx.Done():
						t.Fatal("PATCH did not read the job")
					}
					advanced := stored.NextRunAt.Add(5 * time.Minute)
					var next *time.Time = &advanced
					state := "paused"
					if test.nilNext {
						next, state = nil, "dead"
					}
					scheduled := store.Run{ID: "scheduler-run", JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: *stored.NextRunAt}
					if err := st.ScheduleJobRun(ctx, job.ID, scheduled, next); err != nil {
						t.Fatal(err)
					}
					if err := st.SetJobStatus(ctx, job.ID, state); err != nil {
						t.Fatal(err)
					}
					close(wrapped.proceed)
					var w *httptest.ResponseRecorder
					select {
					case w = <-done:
					case <-ctx.Done():
						t.Fatal("PATCH did not finish")
					}
					if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
						t.Fatalf("PATCH: %d %s", w.Code, w.Body.String())
					}
					if wrapped.calls != 1 || wrapped.scheduleChanged != test.changed {
						t.Fatalf("configuration calls=%d scheduleChanged=%v", wrapped.calls, wrapped.scheduleChanged)
					}
					if test.changed {
						at, _ := time.Parse(time.RFC3339, "2099-01-02T03:04:05Z")
						next = &at
					}
					updated, err := st.GetJob(ctx, job.ID)
					if err != nil {
						t.Fatal(err)
					}
					if updated.Status != state || !reflect.DeepEqual(optionalTimestamp(updated.NextRunAt), optionalTimestamp(next)) || updated.Name != "renamed" || !updated.CreatedAt.Equal(stored.CreatedAt) {
						t.Fatalf("persisted configuration/state=%+v", updated)
					}
					response := decode[jobDTO](t, w)
					if !reflect.DeepEqual(response, jobResponse(updated)) {
						t.Fatalf("stale PATCH response: %+v want %+v", response, jobResponse(updated))
					}
				})
			}
		})
	}
}

type legacyStore struct {
	store.Store
}

func TestPatchLegacyStoreFallback(t *testing.T) {
	st, _ := setup(t)
	handler := New(legacyStore{Store: st}, testToken, nil)
	job := create(t, handler)
	paused := decode[jobDTO](t, request(t, handler, "POST", "/api/jobs/"+job.ID+"/pause", "", 200))
	patched := decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"name":"legacy"}`, 200))
	if patched.Name != "legacy" || patched.Status != "paused" || !reflect.DeepEqual(patched.NextRunAt, paused.NextRunAt) {
		t.Fatalf("legacy patch=%+v", patched)
	}
	patched = decode[jobDTO](t, request(t, handler, "PATCH", "/api/jobs/"+job.ID, `{"schedule":"once:2099-01-02T03:04:05Z"}`, 200))
	if patched.Status != "paused" || patched.NextRunAt == nil || *patched.NextRunAt != "2099-01-02T03:04:05Z" {
		t.Fatalf("legacy schedule patch=%+v", patched)
	}
}

type configurationErrorStore struct {
	store.Store
	err error
}

func (s configurationErrorStore) UpdateJobConfiguration(context.Context, store.Job, bool) error {
	return s.err
}

func TestPatchConfigurationErrors(t *testing.T) {
	st, handler := setup(t)
	job := create(t, handler)
	for _, test := range []struct {
		err  error
		code int
	}{
		{err: sql.ErrNoRows, code: 404},
		{err: errors.New("private configuration error"), code: 500},
	} {
		h := New(configurationErrorStore{Store: st, err: test.err}, testToken, slog.New(slog.NewTextHandler(io.Discard, nil)))
		w := request(t, h, "PATCH", "/api/jobs/"+job.ID, `{"name":"changed"}`, test.code)
		if strings.Contains(w.Body.String(), "private") {
			t.Fatal("configuration error details leaked")
		}
		got := decode[jobDTO](t, request(t, handler, "GET", "/api/jobs/"+job.ID, "", 200))
		if !reflect.DeepEqual(got, job) {
			t.Fatal("failed configuration update fell back to full update")
		}
	}
}

func TestCacheControlOnRouterResponses(t *testing.T) {
	_, handler := setup(t)
	for _, path := range []string{"/api", "/api/unknown", "/api/jobs/"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("router cache control for %s=%q", path, w.Header().Get("Cache-Control"))
		}
	}
}
