package alerts

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

func newWebhook(t *testing.T, url, secret string, statuses []string) *Webhook {
	t.Helper()
	w, err := New(url, secret, statuses, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestNewValidation(t *testing.T) {
	for _, rawURL := range []string{"", "example.com/hook", "//example.com/hook", "/hook", "ftp://example.com/hook", "http:///hook", "https://", "http://:80/hook", "https://user:password@example.com/hook", "https://user@example.com/hook", "https://example.com/hook#fragment", "https://example.com/hook#", "https://example.com:bad/hook", "https://example.com/%zz"} {
		t.Run(rawURL, func(t *testing.T) {
			w, err := New(rawURL, "secret", nil, nil)
			if err == nil || w != nil {
				t.Fatalf("accepted invalid URL: %q", rawURL)
			}
			if strings.Contains(err.Error(), rawURL) && rawURL != "" {
				t.Fatalf("error disclosed URL: %v", err)
			}
		})
	}
	for _, rawURL := range []string{"http://example.com", "https://example.com/hook?token=private", "http://[::1]:8080/hook"} {
		w, err := New(rawURL, "", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if w.client.Timeout != 5*time.Second || w.logger == nil || !reflect.DeepEqual(w.statuses, map[string]bool{"dead": true}) {
			t.Fatalf("unexpected defaults: %+v", w)
		}
	}
	for _, statuses := range [][]string{{"running"}, {"dead", "FAILED"}, {""}} {
		if _, err := New("http://example.com", "", statuses, nil); err == nil {
			t.Fatalf("accepted statuses %v", statuses)
		}
	}
}

func TestNotifyPayloadAndSignature(t *testing.T) {
	secret := "signing-secret"
	command := "echo command-with-private-credentials"
	finished := time.Date(2026, 9, 30, 12, 30, 0, 0, time.UTC)
	exitCode := 1
	duration := int64(1234)
	job := store.Job{ID: "job-1", Name: "nightly", Command: command, Schedule: "private-schedule"}
	run := store.Run{ID: "run-1", JobID: job.ID, Status: "dead", Attempt: 3, ExitCode: &exitCode, DurationMS: &duration, Error: "exit status 1", FinishedAt: &finished}
	var bodies [][]byte
	var signatures, ids []string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Error("unexpected request method or content type")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		signatures = append(signatures, r.Header.Get("X-AQS-Signature"))
		ids = append(ids, r.Header.Get("X-AQS-Event-ID"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	w := newWebhook(t, server.URL+"/hook?credential=query-secret", secret, nil)
	for i := 0; i < 2; i++ {
		if err := w.Notify(context.Background(), job, run); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || ids[0] != "run-1:dead" || ids[1] != ids[0] {
		t.Fatalf("event identity is not stable: %v", ids)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(bodies[0])
	wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if signatures[0] != wantSignature || signatures[1] != wantSignature {
		t.Fatalf("incorrect signatures: %v", signatures)
	}
	var got map[string]any
	if err := json.Unmarshal(bodies[0], &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"event_id": "run-1:dead", "event": "run.dead", "job_id": "job-1", "job_name": "nightly", "run_id": "run-1", "status": "dead", "attempt": float64(3), "exit_code": float64(1), "duration_ms": float64(1234), "error": "exit status 1", "finished_at": finished.Format(time.RFC3339),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload mismatch: got %v, want %v", got, want)
	}
	for _, private := range []string{command, secret, "query-secret", "private-schedule", "command", "logs", "credentials"} {
		if bytes.Contains(bodies[0], []byte(private)) {
			t.Fatalf("payload contains private field or value %q", private)
		}
	}
}

func TestNotifyStatusesAndUnsignedPayload(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if _, present := r.Header["X-Aqs-Signature"]; present {
			t.Error("unsigned webhook sent a signature header")
		}
		var payload event
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload.Event != "run."+payload.Status || payload.EventID != "run:"+payload.Status {
			t.Errorf("incorrect event: %+v", payload)
		}
		if payload.ExitCode != nil || payload.DurationMS != nil || payload.FinishedAt != nil {
			t.Error("missing metadata did not stay null")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	for _, statuses := range [][]string{nil, {}, {"failed", "dead", "succeeded"}, {"failed", "failed"}} {
		w := newWebhook(t, server.URL, "", statuses)
		for _, status := range []string{"pending", "running", "failed", "dead", "succeeded", ""} {
			before := calls.Load()
			if err := w.Notify(context.Background(), store.Job{}, store.Run{ID: "run", Status: status}); err != nil {
				t.Fatal(err)
			}
			want := before
			if w.statuses[status] {
				want++
			}
			if calls.Load() != want {
				t.Fatalf("status %q: got %d calls, want %d", status, calls.Load(), want)
			}
		}
	}
}

func TestNotifyHTTPRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		codes    []int
		wantErr  bool
		attempts int32
	}{
		{"rate limited", []int{429, 204}, false, 2},
		{"server errors", []int{500, 503, 200}, false, 3},
		{"exhausted", []int{503, 503, 503}, true, 3},
		{"bad request", []int{400}, true, 1},
		{"unauthorized", []int{401}, true, 1},
		{"forbidden", []int{403}, true, 1},
		{"not found", []int{404}, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var mu sync.Mutex
			var firstBody, firstID, firstSignature string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				if n == 1 {
					firstBody, firstID, firstSignature = string(body), r.Header.Get("X-AQS-Event-ID"), r.Header.Get("X-AQS-Signature")
				} else if string(body) != firstBody || r.Header.Get("X-AQS-Event-ID") != firstID || r.Header.Get("X-AQS-Signature") != firstSignature {
					t.Error("retry changed event or signature")
				}
				mu.Unlock()
				index := int(n) - 1
				if index >= len(tc.codes) {
					index = len(tc.codes) - 1
				}
				w.WriteHeader(tc.codes[index])
				_, _ = io.WriteString(w, "private-response-secret")
			}))
			defer server.Close()
			w := newWebhook(t, server.URL+"/private-path?token=private-query", "private-signing-secret", nil)
			err := w.Notify(context.Background(), store.Job{}, store.Run{ID: "run", Status: "dead"})
			if (err != nil) != tc.wantErr || calls.Load() != tc.attempts {
				t.Fatalf("got error %v, attempts %d", err, calls.Load())
			}
			if err != nil {
				for _, private := range []string{server.URL, "private-path", "private-query", "private-signing-secret", "private-response-secret"} {
					if strings.Contains(err.Error(), private) {
						t.Fatalf("error leaks private data: %v", err)
					}
				}
			}
		})
	}
}

func TestNotifyRedirectNotFollowed(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	for _, code := range []int{301, 302, 303, 307, 308} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			http.Redirect(w, r, target.URL+"/private-redirect", code)
		}))
		w := newWebhook(t, server.URL, "private-secret", nil)
		err := w.Notify(context.Background(), store.Job{}, store.Run{Status: "dead"})
		server.Close()
		if err == nil || calls.Load() != 1 || targetCalls.Load() != 0 {
			t.Fatalf("redirect %d: error %v, source calls %d, target calls %d", code, err, calls.Load(), targetCalls.Load())
		}
		if strings.Contains(err.Error(), target.URL) || strings.Contains(err.Error(), "private") {
			t.Fatalf("redirect error leaks data: %v", err)
		}
	}
}

func TestNotifyCancellation(t *testing.T) {
	t.Run("before request", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
		defer server.Close()
		w := newWebhook(t, server.URL, "", nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := w.Notify(ctx, store.Job{}, store.Run{Status: "dead"}); !errors.Is(err, context.Canceled) || calls.Load() != 0 {
			t.Fatalf("error %v, calls %d", err, calls.Load())
		}
		if err := w.Notify(ctx, store.Job{}, store.Run{Status: "succeeded"}); err != nil {
			t.Fatalf("ignored status returned an error: %v", err)
		}
	})
	t.Run("during backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()
		w := newWebhook(t, server.URL, "", nil)
		w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := http.DefaultTransport.RoundTrip(r)
			if err == nil {
				resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			}
			return resp, err
		})
		started := time.Now()
		if err := w.Notify(ctx, store.Job{}, store.Run{Status: "dead"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
		if calls.Load() != 1 || time.Since(started) > time.Second {
			t.Fatalf("cancellation was not prompt; calls %d", calls.Load())
		}
	})
	t.Run("during request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			<-r.Context().Done()
		}))
		defer server.Close()
		w := newWebhook(t, server.URL, "", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := w.Notify(ctx, store.Job{}, store.Run{Status: "dead"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline, got %v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("request cancellation was not prompt")
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	b.cancel()
	return b.ReadCloser.Close()
}

func TestNotifyNetworkRetries(t *testing.T) {
	for _, succeed := range []bool{false, true} {
		w := newWebhook(t, "http://example.com/private?secret=query", "signing-secret", nil)
		calls := 0
		w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if succeed && calls == 3 {
				return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
			}
			return nil, errors.New("private network error http://example.com/private?secret=query signing-secret")
		})
		err := w.Notify(context.Background(), store.Job{}, store.Run{Status: "dead"})
		if calls != 3 || (err == nil) != succeed {
			t.Fatalf("got error %v, calls %d", err, calls)
		}
		if err != nil && err.Error() != "webhook network delivery failed" {
			t.Fatalf("network error not sanitized: %v", err)
		}
	}
}

type trackedBody struct {
	read   int
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

func TestNotifyResponseReadBoundedAndClosed(t *testing.T) {
	for _, status := range []int{200, 400, 503, 307} {
		w := newWebhook(t, "http://example.com", "", nil)
		var bodies []*trackedBody
		w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := &trackedBody{}
			bodies = append(bodies, body)
			return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
		})
		_ = w.Notify(context.Background(), store.Job{}, store.Run{Status: "dead"})
		for _, body := range bodies {
			if !body.closed || body.read != maxResponseBytes {
				t.Fatalf("HTTP %d: closed %v, read %d", status, body.closed, body.read)
			}
		}
	}
}

func TestHandlerDeadlineAndSafeLogging(t *testing.T) {
	var logs bytes.Buffer
	w, err := New("http://example.com/private-path?secret=query-secret", "signing-secret", nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) < 4*time.Second {
			t.Error("handler request does not have a five-second deadline")
		}
		return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader("response-secret")), Header: make(http.Header)}, nil
	})
	w.Handler()(store.Job{ID: "job", Command: "command-secret"}, store.Run{ID: "run", Status: "dead", Error: "run-secret"})
	if calls != 1 {
		t.Fatalf("got %d calls", calls)
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "webhook delivery failed" || record["job_id"] != "job" || record["run_id"] != "run" || record["status"] != "dead" || len(record) != 6 {
		t.Fatalf("unexpected log: %v", record)
	}
	for _, private := range []string{"example.com", "private-path", "query-secret", "signing-secret", "response-secret", "command-secret", "run-secret"} {
		if strings.Contains(logs.String(), private) {
			t.Fatalf("handler log leaks %q", private)
		}
	}
	logs.Reset()
	w.Handler()(store.Job{}, store.Run{Status: "succeeded"})
	if logs.Len() != 0 || calls != 1 {
		t.Fatal("ignored status logged or delivered")
	}
}

func TestHandlerOverallTimeout(t *testing.T) {
	w := newWebhook(t, "http://example.com", "", nil)
	calls := 0
	w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	started := time.Now()
	w.Handler()(store.Job{}, store.Run{Status: "dead"})
	elapsed := time.Since(started)
	if calls != 1 || elapsed < 4*time.Second || elapsed > 6*time.Second {
		t.Fatalf("handler exceeded overall budget: elapsed %v, calls %d", elapsed, calls)
	}
}
