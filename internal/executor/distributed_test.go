package executor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
	"github.com/alicebob/miniredis/v2"
)

type observedDistributed struct {
	store.DistributedStore
	mu        sync.Mutex
	owners    []string
	renew     func(context.Context, string, string, time.Time, time.Duration) (bool, error)
	complete  func(context.Context, store.Run, string, *store.Run) error
	appendLog func(context.Context, string, store.Log) error
}

func (s *observedDistributed) ClaimRun(context.Context, string, time.Time) (bool, error) {
	return false, errors.New("unfenced claim")
}

func (s *observedDistributed) UpdateRun(context.Context, store.Run) error {
	return errors.New("unfenced update")
}

func (s *observedDistributed) CreateRun(context.Context, store.Run) error {
	return errors.New("non-atomic retry")
}

func (s *observedDistributed) AppendLog(context.Context, store.Log) error {
	return errors.New("unfenced log")
}

func (s *observedDistributed) RunningRuns(context.Context) ([]store.Run, error) {
	return nil, errors.New("bulk recovery")
}

func (s *observedDistributed) ClaimRunLease(ctx context.Context, id, owner string, now time.Time, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	s.owners = append(s.owners, owner)
	s.mu.Unlock()
	return s.DistributedStore.ClaimRunLease(ctx, id, owner, now, ttl)
}

func (s *observedDistributed) RenewRunLease(ctx context.Context, id, owner string, now time.Time, ttl time.Duration) (bool, error) {
	if s.renew != nil {
		return s.renew(ctx, id, owner, now, ttl)
	}
	return s.DistributedStore.RenewRunLease(ctx, id, owner, now, ttl)
}

func (s *observedDistributed) CompleteRun(ctx context.Context, run store.Run, owner string, retry *store.Run) error {
	if s.complete != nil {
		return s.complete(ctx, run, owner, retry)
	}
	return s.DistributedStore.CompleteRun(ctx, run, owner, retry)
}

func (s *observedDistributed) AppendRunLog(ctx context.Context, owner string, entry store.Log) error {
	if s.appendLog != nil {
		return s.appendLog(ctx, owner, entry)
	}
	return s.DistributedStore.AppendRunLog(ctx, owner, entry)
}

func redisExecutorStore(t *testing.T) (*store.Redis, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	st, err := store.OpenRedis("redis://"+server.Addr(), "executor-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st, server
}

func waitExecutor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for executor")
}

func awaitProcess(t *testing.T, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("worker did not return")
	}
}

func startProcess(t *testing.T, p *Pool, run store.Run) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.process(ctx, run)
	}()
	t.Cleanup(func() {
		cancel()
		awaitProcess(t, done, 5*time.Second)
	})
	return done
}

func shortLease(t *testing.T, p *Pool) {
	t.Helper()
	if err := p.SetLeaseTTL(3 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseTTLConfiguration(t *testing.T) {
	p := testPool(newTestStore(t))
	if p.ttl() != 15*time.Second {
		t.Fatalf("default TTL = %s", p.ttl())
	}
	for _, ttl := range []time.Duration{-time.Second, 0, 3*time.Second - 1} {
		if err := p.SetLeaseTTL(ttl); err == nil {
			t.Fatalf("accepted TTL %s", ttl)
		}
	}
	shortLease(t, p)
	if p.ttl() != 3*time.Second {
		t.Fatalf("TTL = %s", p.ttl())
	}
}

func TestDistributedPoolsExecuteOnce(t *testing.T) {
	st, server := redisExecutorStore(t)
	other, err := store.OpenRedis("redis://"+server.Addr(), "executor-test")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	path := filepath.Join(t.TempDir(), "executions")
	_, run := createRun(t, st, fmt.Sprintf("printf x >> %q; sleep 0.2; printf done", path), 5, 1)
	var results atomic.Int32
	pools := []*Pool{testPool(&observedDistributed{DistributedStore: st}), testPool(&observedDistributed{DistributedStore: other})}
	for _, p := range pools {
		p.poll = 10 * time.Millisecond
		shortLease(t, p)
		p.SetResultHandler(func(_ store.Job, result store.Run) {
			if result.Status != "succeeded" {
				t.Errorf("result = %#v", result)
			}
			results.Add(1)
		})
		p.Start()
		t.Cleanup(p.Abort)
	}
	waitExecutor(t, 5*time.Second, func() bool { return results.Load() == 1 })
	for _, p := range pools {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := p.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "x" || results.Load() != 1 {
		t.Fatalf("executions=%q results=%d err=%v", data, results.Load(), err)
	}
	logs, err := st.ListLogs(context.Background(), run.ID)
	if err != nil || len(logs) != 1 || logs[0].Chunk != "done" {
		t.Fatalf("logs=%#v err=%v", logs, err)
	}
}

func TestDistributedRenewalAndLiveRecovery(t *testing.T) {
	st, _ := redisExecutorStore(t)
	_, run := createRun(t, st, "sleep 4; printf renewed", 10, 1)
	var renewals atomic.Int32
	observed := &observedDistributed{DistributedStore: st}
	observed.renew = func(ctx context.Context, id, owner string, now time.Time, ttl time.Duration) (bool, error) {
		owned, err := st.RenewRunLease(ctx, id, owner, now, ttl)
		if owned && err == nil {
			renewals.Add(1)
		}
		return owned, err
	}
	p := testPool(observed)
	shortLease(t, p)
	done := startProcess(t, p, run)
	waitExecutor(t, 2*time.Second, func() bool { return renewals.Load() > 0 })
	other := testPool(&observedDistributed{DistributedStore: st})
	if err := other.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2200 * time.Millisecond)
	if err := other.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "running" || got.Attempt != 1 {
		t.Fatalf("live run = %#v err=%v", got, err)
	}
	awaitProcess(t, done, 3*time.Second)
	got, err = st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "succeeded" || renewals.Load() < 3 {
		t.Fatalf("renewed run = %#v renewals=%d err=%v", got, renewals.Load(), err)
	}
}

func TestDistributedLeaseLossKillsProcessGroup(t *testing.T) {
	st, server := redisExecutorStore(t)
	dir := t.TempDir()
	pidPath, marker := filepath.Join(dir, "pid"), filepath.Join(dir, "survived")
	command := fmt.Sprintf("printf '%%s' $$ > %q; (sleep 3; printf survived > %q) & wait", pidPath, marker)
	_, run := createRun(t, st, command, 20, 3)
	var results atomic.Int32
	p := testPool(st)
	shortLease(t, p)
	p.SetResultHandler(func(store.Job, store.Run) { results.Add(1) })
	done := startProcess(t, p, run)
	var pid int
	waitExecutor(t, 2*time.Second, func() bool {
		data, err := os.ReadFile(pidPath)
		if err == nil {
			pid, err = strconv.Atoi(string(data))
		}
		return err == nil && pid > 0
	})
	server.Del("executor-test:leases:" + run.ID)
	awaitProcess(t, done, 2*time.Second)
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("command still alive: %v", err)
	}
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "running" || got.FinishedAt != nil || results.Load() != 0 {
		t.Fatalf("stale completion persisted: %#v results=%d err=%v", got, results.Load(), err)
	}
	runs, err := st.ListRuns(context.Background(), run.JobID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("stale retry: %#v err=%v", runs, err)
	}
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child process survived: %v", err)
	}
}

func TestDistributedRenewalFailuresAreBounded(t *testing.T) {
	for _, mode := range []string{"not-owner", "error", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			st, _ := redisExecutorStore(t)
			_, run := createRun(t, st, "sleep 20", 30, 2)
			release := make(chan struct{})
			defer close(release)
			observed := &observedDistributed{DistributedStore: st}
			observed.renew = func(ctx context.Context, _ string, _ string, _ time.Time, _ time.Duration) (bool, error) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 500*time.Millisecond {
					t.Error("renewal timeout must be less than renewal interval")
				}
				switch mode {
				case "error":
					return false, errors.New("redis unavailable")
				case "blocked":
					<-release
					return true, nil
				default:
					return false, nil
				}
			}
			p := testPool(observed)
			shortLease(t, p)
			var results atomic.Int32
			p.SetResultHandler(func(store.Job, store.Run) { results.Add(1) })
			started := time.Now()
			done := startProcess(t, p, run)
			awaitProcess(t, done, 2500*time.Millisecond)
			if time.Since(started) > 2*time.Second {
				t.Fatal("renewal failure did not promptly cancel command")
			}
			got, err := st.GetRun(context.Background(), run.ID)
			if err != nil || got.Status != "running" || results.Load() != 0 {
				t.Fatalf("stale result = %#v results=%d err=%v", got, results.Load(), err)
			}
		})
	}
}

func TestDistributedDispatcherRequeuesAndPreservesLogs(t *testing.T) {
	st, server := redisExecutorStore(t)
	_, run := createRun(t, st, "printf resumed", 5, 3)
	ctx := context.Background()
	claimed, err := st.ClaimRunLease(ctx, run.ID, "expired-owner", time.Now(), 3*time.Second)
	if err != nil || !claimed {
		t.Fatalf("claim = %t err=%v", claimed, err)
	}
	if err := st.AppendRunLog(ctx, "expired-owner", store.Log{RunID: run.ID, Stream: "stdout", Chunk: "preserved", Seq: 7}); err != nil {
		t.Fatal(err)
	}
	server.FastForward(4 * time.Second)
	observed := &observedDistributed{DistributedStore: st}
	p := testPool(observed)
	p.poll = 10 * time.Millisecond
	shortLease(t, p)
	p.Start()
	t.Cleanup(p.Abort)
	waitExecutor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(ctx, run.ID)
		return err == nil && got.Status == "succeeded"
	})
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetRun(ctx, run.ID)
	if err != nil || got.Attempt != 2 {
		t.Fatalf("requeued run = %#v err=%v", got, err)
	}
	logs, err := st.ListLogs(ctx, run.ID)
	if err != nil || len(logs) != 2 || logs[0].Seq != 7 || logs[0].Chunk != "preserved" || logs[1].Seq != 8 || logs[1].Chunk != "resumed" {
		t.Fatalf("preserved logs = %#v err=%v", logs, err)
	}
	if err := st.AppendRunLog(ctx, "expired-owner", store.Log{RunID: run.ID, Seq: 9, Chunk: "stale"}); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale log error = %v", err)
	}
	retry := store.Run{ID: "stale-retry", JobID: run.JobID, Status: "pending", Attempt: 3}
	if err := st.CompleteRun(ctx, got, "expired-owner", &retry); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v", err)
	}
	runs, err := st.ListRuns(ctx, run.JobID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("stale retry persisted = %#v err=%v", runs, err)
	}
}

func TestDistributedAtomicRetryAndUniqueTokens(t *testing.T) {
	st, _ := redisExecutorStore(t)
	job, run := createRun(t, st, "printf failed; exit 1", 5, 2)
	observed := &observedDistributed{DistributedStore: st}
	var completions atomic.Int32
	observed.complete = func(ctx context.Context, result store.Run, owner string, retry *store.Run) error {
		if result.Attempt == 1 && (result.Status != "failed" || retry == nil || retry.Attempt != 2 || retry.Status != "pending") {
			t.Errorf("non-atomic retry: result=%#v retry=%#v", result, retry)
		}
		if result.Attempt == 2 && (result.Status != "dead" || retry != nil) {
			t.Errorf("retry limit: result=%#v retry=%#v", result, retry)
		}
		completions.Add(1)
		return st.CompleteRun(ctx, result, owner, retry)
	}
	p := testPool(observed)
	p.rng = rand.New(rand.NewSource(42))
	var statuses []string
	p.SetResultHandler(func(gotJob store.Job, result store.Run) {
		persisted, err := st.GetRun(context.Background(), result.ID)
		if err != nil || gotJob.ID != job.ID || persisted.Status != result.Status || result.FinishedAt == nil || result.ExitCode == nil || *result.ExitCode != 1 {
			t.Errorf("callback result=%#v persisted=%#v err=%v", result, persisted, err)
		}
		statuses = append(statuses, result.Status)
	})
	p.process(context.Background(), run)
	runs, err := st.ListRuns(context.Background(), job.ID)
	if err != nil || len(runs) != 2 {
		t.Fatalf("atomic retry runs=%#v err=%v", runs, err)
	}
	for _, next := range runs {
		if next.Status == "pending" {
			p.process(context.Background(), next)
		}
	}
	if strings.Join(statuses, ",") != "failed,dead" || completions.Load() != 2 {
		t.Fatalf("results=%v completions=%d", statuses, completions.Load())
	}
	observed.mu.Lock()
	defer observed.mu.Unlock()
	if len(observed.owners) != 2 || observed.owners[0] == "" || observed.owners[0] == observed.owners[1] {
		t.Fatalf("per-claim tokens = %v", observed.owners)
	}
}

func TestDistributedStaleFinalAndRetryAreRefused(t *testing.T) {
	st, server := redisExecutorStore(t)
	_, run := createRun(t, st, "printf old; exit 1", 5, 3)
	observed := &observedDistributed{DistributedStore: st}
	observed.complete = func(ctx context.Context, result store.Run, owner string, retry *store.Run) error {
		if retry == nil {
			t.Error("retry must be included in completion")
		}
		server.FastForward(20 * time.Second)
		if err := st.RecoverExpiredRuns(ctx, time.Now()); err != nil {
			return err
		}
		claimed, err := st.ClaimRunLease(ctx, run.ID, "replacement", time.Now(), 15*time.Second)
		if err != nil || !claimed {
			return fmt.Errorf("replacement claim=%t err=%v", claimed, err)
		}
		err = st.CompleteRun(ctx, result, owner, retry)
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Errorf("stale completion error=%v", err)
		}
		return err
	}
	p := testPool(observed)
	var results atomic.Int32
	p.SetResultHandler(func(store.Job, store.Run) { results.Add(1) })
	p.process(context.Background(), run)
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "running" || got.Attempt != 2 || got.FinishedAt != nil || results.Load() != 0 {
		t.Fatalf("replacement overwritten: %#v results=%d err=%v", got, results.Load(), err)
	}
	runs, err := st.ListRuns(context.Background(), run.JobID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("stale atomic retry persisted: %#v err=%v", runs, err)
	}
}

func TestDistributedStaleLogCancelsExecution(t *testing.T) {
	st, server := redisExecutorStore(t)
	_, run := createRun(t, st, "printf '%02000d' 0; sleep 20", 30, 2)
	observed := &observedDistributed{DistributedStore: st}
	var rejected atomic.Int32
	observed.appendLog = func(ctx context.Context, owner string, entry store.Log) error {
		server.Del("executor-test:leases:" + run.ID)
		err := st.AppendRunLog(ctx, owner, entry)
		if errors.Is(err, store.ErrLeaseLost) {
			rejected.Add(1)
		}
		return err
	}
	p := testPool(observed)
	var results atomic.Int32
	p.SetResultHandler(func(store.Job, store.Run) { results.Add(1) })
	done := startProcess(t, p, run)
	awaitProcess(t, done, 2*time.Second)
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil || got.Status != "running" || results.Load() != 0 || rejected.Load() == 0 {
		t.Fatalf("stale log result=%#v alerts=%d rejected=%d err=%v", got, results.Load(), rejected.Load(), err)
	}
	logs, err := st.ListLogs(context.Background(), run.ID)
	if err != nil || len(logs) != 0 {
		t.Fatalf("stale logs=%#v err=%v", logs, err)
	}
}

func TestDistributedReaperContinuesWithFullDispatchQueue(t *testing.T) {
	st, server := redisExecutorStore(t)
	job, run := createRun(t, st, "sleep 20", 30, 1)
	ctx := context.Background()
	for _, id := range []string{"queued-1", "queued-2", "expired"} {
		next := store.Run{ID: id, JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: time.Now(), CreatedAt: time.Now()}
		if err := st.CreateRun(ctx, next); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := st.ClaimRunLease(ctx, "expired", "old", time.Now(), 15*time.Second)
	if err != nil || !claimed {
		t.Fatalf("claim=%t err=%v", claimed, err)
	}
	p := testPool(st)
	p.poll = 10 * time.Millisecond
	p.Start()
	t.Cleanup(p.Abort)
	waitExecutor(t, 2*time.Second, func() bool {
		got, err := st.GetRun(ctx, run.ID)
		return err == nil && got.Status == "running"
	})
	time.Sleep(100 * time.Millisecond)
	server.Del("executor-test:leases:expired")
	waitExecutor(t, time.Second, func() bool {
		got, err := st.GetRun(ctx, "expired")
		return err == nil && got.Status == "pending" && got.Attempt == 2
	})
}

func TestPoolAbortPreservesPersistedResult(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var st store.Store
			if backend == "sqlite" {
				st = newTestStore(t)
			} else {
				st, _ = redisExecutorStore(t)
			}
			_, run := createRun(t, st, "printf buffered; sleep 20", 30, 3)
			p := testPool(st)
			p.poll = 10 * time.Millisecond
			results := make(chan store.Run, 1)
			p.SetResultHandler(func(_ store.Job, result store.Run) { results <- result })
			p.Start()
			t.Cleanup(p.Abort)
			waitExecutor(t, 2*time.Second, func() bool {
				got, err := st.GetRun(context.Background(), run.ID)
				return err == nil && got.Status == "running"
			})
			time.Sleep(100 * time.Millisecond)
			p.Abort()
			got, err := st.GetRun(context.Background(), run.ID)
			if err != nil || got.Status != "failed" || got.Error != "abandoned during shutdown" {
				t.Fatalf("aborted run=%#v err=%v", got, err)
			}
			select {
			case result := <-results:
				if result.Status != got.Status || result.FinishedAt == nil {
					t.Fatalf("aborted result=%#v", result)
				}
			default:
				t.Fatal("aborted result handler was not called")
			}
			runs, err := st.ListRuns(context.Background(), run.JobID)
			if err != nil || len(runs) != 1 {
				t.Fatalf("shutdown retry=%#v err=%v", runs, err)
			}
			logs, err := st.ListLogs(context.Background(), run.ID)
			if err != nil || len(logs) != 1 || logs[0].Chunk != "buffered" {
				t.Fatalf("shutdown logs=%#v err=%v", logs, err)
			}
		})
	}
}

func TestLocalResultHandlerReceivesFinalRow(t *testing.T) {
	for _, command := range []string{"printf success", "exit 7"} {
		t.Run(command, func(t *testing.T) {
			st := newTestStore(t)
			job, run := createRun(t, st, command, 5, 1)
			p := testPool(st)
			var results []store.Run
			p.SetResultHandler(func(gotJob store.Job, result store.Run) {
				if gotJob.ID != job.ID {
					t.Errorf("job=%#v", gotJob)
				}
				results = append(results, result)
			})
			p.process(context.Background(), run)
			got, err := st.GetRun(context.Background(), run.ID)
			if err != nil || len(results) != 1 || results[0].Status != got.Status || results[0].FinishedAt == nil || results[0].ExitCode == nil || results[0].DurationMS == nil {
				t.Fatalf("results=%#v row=%#v err=%v", results, got, err)
			}
			if command == "exit 7" && results[0].Status != "dead" {
				t.Fatalf("callback ran before dead marking: %#v", results[0])
			}
		})
	}
}
