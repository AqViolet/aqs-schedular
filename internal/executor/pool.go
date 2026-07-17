package executor

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

const logChunkSize = 1024

type Pool struct {
	store         store.Store
	logger        *slog.Logger
	concurrency   int
	poll          time.Duration
	stop          chan struct{}
	done          chan struct{}
	dispatchDone  chan struct{}
	cancelExec    context.CancelFunc
	stopOnce      sync.Once
	rngMu         sync.Mutex
	rng           *mrand.Rand
	configMu      sync.RWMutex
	leaseTTL      time.Duration
	resultHandler func(store.Job, store.Run)
}

func New(st store.Store, concurrency int, logger *slog.Logger) *Pool {
	if concurrency < 1 {
		concurrency = 4
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		store:       st,
		logger:      logger,
		concurrency: concurrency,
		poll:        250 * time.Millisecond,
		leaseTTL:    15 * time.Second,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		rng:         mrand.New(mrand.NewSource(time.Now().UnixNano())),
	}
}

func (p *Pool) SetLeaseTTL(ttl time.Duration) error {
	if ttl < 3*time.Second {
		return fmt.Errorf("lease TTL must be at least 3s")
	}
	p.configMu.Lock()
	p.leaseTTL = ttl
	p.configMu.Unlock()
	return nil
}

func (p *Pool) SetResultHandler(handler func(store.Job, store.Run)) {
	p.configMu.Lock()
	p.resultHandler = handler
	p.configMu.Unlock()
}

func (p *Pool) ttl() time.Duration {
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.leaseTTL
}

func (p *Pool) notify(job store.Job, run store.Run) {
	if run.Status != "dead" && run.Status != "succeeded" && run.Status != "failed" {
		return
	}
	p.configMu.RLock()
	handler := p.resultHandler
	p.configMu.RUnlock()
	if handler != nil {
		handler(job, run)
	}
}

func (p *Pool) Recover(ctx context.Context) error {
	if distributed, ok := p.store.(store.DistributedStore); ok {
		_, err := leaseCall(ctx, p.ttl(), func(ctx context.Context) (struct{}, error) {
			return struct{}{}, distributed.RecoverExpiredRuns(ctx, time.Now())
		})
		return err
	}
	runs, err := p.store.RunningRuns(ctx)
	if err != nil {
		return err
	}
	for _, run := range runs {
		job, err := p.store.GetJob(ctx, run.JobID)
		if err != nil {
			return fmt.Errorf("load interrupted run job %s: %w", run.JobID, err)
		}
		finished := time.Now()
		run.Status = "failed"
		if run.Attempt >= job.MaxRetries {
			run.Status = "dead"
		}
		run.FinishedAt = &finished
		run.Error = "interrupted by daemon restart"
		if run.StartedAt != nil {
			duration := finished.Sub(*run.StartedAt).Milliseconds()
			run.DurationMS = &duration
		}
		if err := p.store.UpdateRun(ctx, run); err != nil {
			return fmt.Errorf("recover run %s: %w", run.ID, err)
		}
		if run.Status == "failed" {
			run, err = p.retry(job, run)
			if err != nil {
				return err
			}
		}
		p.notify(job, run)
		p.logger.Warn("recovered interrupted run", "run_id", run.ID, "job_id", run.JobID, "status", run.Status)
	}
	return nil
}

func (p *Pool) Start() {
	execCtx, cancel := context.WithCancel(context.Background())
	p.cancelExec = cancel
	jobs := make(chan store.Run, p.concurrency)
	var workers sync.WaitGroup
	for i := 0; i < p.concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-execCtx.Done():
					return
				case run, ok := <-jobs:
					if !ok {
						return
					}
					select {
					case <-p.stop:
						return
					default:
					}
					p.process(execCtx, run)
				}
			}
		}()
	}
	if _, ok := p.store.(store.DistributedStore); ok {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p.reap(execCtx)
		}()
	}
	p.dispatchDone = make(chan struct{})
	go func() {
		defer close(p.dispatchDone)
		defer close(jobs)
		for {
			select {
			case <-p.stop:
				return
			default:
			}

			var runs []store.Run
			var err error
			if _, ok := p.store.(store.DistributedStore); ok {
				runs, err = leaseCall(execCtx, p.ttl(), func(ctx context.Context) ([]store.Run, error) {
					return p.store.PendingRuns(ctx, p.concurrency*2)
				})
			} else {
				runs, err = p.store.PendingRuns(execCtx, p.concurrency*2)
			}
			if err != nil {
				p.logger.Error("load pending runs", "error", err)
				select {
				case <-p.stop:
					return
				case <-execCtx.Done():
					return
				case <-time.After(p.poll):
				}
				continue
			}
			for _, run := range runs {
				select {
				case <-p.stop:
					return
				case <-execCtx.Done():
					return
				case jobs <- run:
				}
			}
			select {
			case <-p.stop:
				return
			case <-execCtx.Done():
				return
			case <-time.After(p.poll):
			}
		}
	}()
	go func() {
		<-p.dispatchDone
		workers.Wait()
		close(p.done)
	}()
}

func (p *Pool) reap(ctx context.Context) {
	for {
		select {
		case <-p.stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		if err := p.Recover(ctx); err != nil {
			p.logger.Error("recover expired runs", "error", err)
		}
		select {
		case <-p.stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(p.poll):
		}
	}
}

func (p *Pool) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
	if p.dispatchDone != nil {
		<-p.dispatchDone
	}
}

func (p *Pool) Abort() {
	p.Stop()
	if p.cancelExec != nil {
		p.cancelExec()
	}
	<-p.done
}

func (p *Pool) Shutdown(ctx context.Context) error {
	p.Stop()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		if p.cancelExec != nil {
			p.cancelExec()
		}
		<-p.done
		return ctx.Err()
	}
}

func (p *Pool) process(parent context.Context, run store.Run) {
	select {
	case <-p.stop:
		return
	default:
	}
	if parent.Err() != nil {
		return
	}
	ctx := context.Background()
	runID := run.ID
	var lease *runLease
	var claimed bool
	var err error
	if distributed, ok := p.store.(store.DistributedStore); ok {
		lease = newRunLease(parent, distributed, run.ID, p.ttl(), p.logger)
		defer lease.close()
		claimed, err = leaseCall(lease.ctx, lease.ttl, func(ctx context.Context) (bool, error) {
			return distributed.ClaimRunLease(ctx, runID, lease.owner, time.Now(), lease.ttl)
		})
		if err == nil && claimed {
			lease.start()
		}
	} else {
		claimed, err = p.store.ClaimRun(ctx, run.ID, time.Now())
	}
	if err != nil {
		p.logger.Error("claim pending run", "run_id", run.ID, "error", err)
		return
	}
	if !claimed {
		return
	}
	if lease != nil {
		run, err = leaseCall(lease.ctx, lease.ttl, func(ctx context.Context) (store.Run, error) {
			return p.store.GetRun(ctx, runID)
		})
	} else {
		run, err = p.store.GetRun(ctx, run.ID)
	}
	if err != nil {
		p.logger.Error("load claimed run", "run_id", run.ID, "error", err)
		return
	}
	var job store.Job
	if lease != nil {
		job, err = leaseCall(lease.ctx, lease.ttl, func(ctx context.Context) (store.Job, error) {
			return p.store.GetJob(ctx, run.JobID)
		})
	} else {
		job, err = p.store.GetJob(ctx, run.JobID)
	}
	if err != nil {
		if finishErr := p.complete(store.Job{}, run, "failed", nil, 0, fmt.Sprintf("load job: %v", err), false, lease); finishErr != nil {
			p.logger.Error("persist missing job result", "run_id", run.ID, "error", finishErr)
		}
		return
	}
	p.execute(parent, job, run, lease)
}

func (p *Pool) execute(parent context.Context, job store.Job, run store.Run, lease *runLease) {
	started := time.Now()
	execParent := parent
	if lease != nil {
		execParent = lease.ctx
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if job.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(execParent, time.Duration(job.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(execParent)
	}
	defer cancel()

	seq := &logSequence{store: p.store, runID: run.ID, lease: lease}
	if lease != nil {
		logs, err := leaseCall(lease.ctx, lease.ttl, func(ctx context.Context) ([]store.Log, error) {
			return p.store.ListLogs(ctx, run.ID)
		})
		if err != nil {
			p.logger.Error("load preserved logs", "run_id", run.ID, "error", err)
			return
		}
		for _, entry := range logs {
			if entry.Seq > seq.seq {
				seq.seq = entry.Seq
			}
		}
	}
	stdout := &chunkWriter{stream: "stdout", sequence: seq}
	stderr := &chunkWriter{stream: "stderr", sequence: seq}
	cmd := exec.CommandContext(ctx, "sh", "-c", job.Command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	outErr := stdout.Close()
	errErr := stderr.Close()
	if err == nil && outErr != nil {
		err = outErr
	}
	if err == nil && errErr != nil {
		err = errErr
	}

	finished := time.Now()
	duration := finished.Sub(started).Milliseconds()
	var exitCode *int
	status, message := "succeeded", ""
	if cmd.ProcessState != nil {
		code := cmd.ProcessState.ExitCode()
		exitCode = &code
	}
	if err != nil {
		status = "failed"
		if ctx.Err() != nil {
			if parent.Err() != nil {
				message = "abandoned during shutdown"
			} else if job.TimeoutSeconds > 0 {
				message = fmt.Sprintf("timeout after %ds", job.TimeoutSeconds)
			} else {
				message = "execution canceled"
			}
		} else {
			message = err.Error()
		}

	}
	if logErr := seq.error(); logErr != nil {
		status = "failed"
		message = "capture logs: " + logErr.Error()
	}
	if err := p.complete(job, run, status, exitCode, duration, message, parent.Err() == nil, lease); err != nil {
		p.logger.Error("persist run result", "run_id", run.ID, "error", err)
		return
	}
	if status == "failed" {
		if parent.Err() != nil {
			p.logger.Warn("run abandoned during shutdown", "run_id", run.ID, "job_id", job.ID)
			return
		}
		p.logger.Warn("run failed", "run_id", run.ID, "job_id", job.ID, "error", message)
		return
	}
	p.logger.Info("run completed", "run_id", run.ID, "job_id", job.ID, "duration_ms", duration)
}

func (p *Pool) complete(job store.Job, run store.Run, status string, exitCode *int, duration int64, message string, allowRetry bool, lease *runLease) error {
	if lease == nil {
		if err := p.finish(run, status, exitCode, duration, message); err != nil {
			return err
		}
		var err error
		if status == "failed" && allowRetry && job.ID != "" {
			run, err = p.retry(job, run)
		} else {
			run, err = p.store.GetRun(context.Background(), run.ID)
		}
		if err != nil {
			return err
		}
		if job.ID != "" {
			p.notify(job, run)
		}
		return nil
	}
	if lease.lost.Load() {
		return store.ErrLeaseLost
	}
	finished := time.Now()
	run.Status = status
	run.FinishedAt = &finished
	run.ExitCode = exitCode
	run.DurationMS = &duration
	run.Error = message
	var retry *store.Run
	if status == "failed" && allowRetry && job.ID != "" {
		if run.Attempt >= job.MaxRetries {
			run.Status = "dead"
		} else {
			next := p.retryRun(job, run)
			retry = &next
		}
	}
	_, err := leaseCall(context.Background(), lease.ttl, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, lease.store.CompleteRun(ctx, run, lease.owner, retry)
	})
	if err != nil {
		return err
	}
	lease.stopRenewal()
	if job.ID != "" {
		p.notify(job, run)
	}
	return nil
}

func (p *Pool) finish(run store.Run, status string, exitCode *int, duration int64, message string) error {
	finished := time.Now()
	run.Status = status
	run.FinishedAt = &finished
	run.ExitCode = exitCode
	run.DurationMS = &duration
	run.Error = message
	return p.store.UpdateRun(context.Background(), run)
}

func (p *Pool) retry(job store.Job, run store.Run) (store.Run, error) {
	var err error
	run, err = p.store.GetRun(context.Background(), run.ID)
	if err != nil {
		return run, fmt.Errorf("load failed run before retry: %w", err)
	}
	if run.Attempt >= job.MaxRetries {
		run.Status = "dead"
		if err := p.store.UpdateRun(context.Background(), run); err != nil {
			return run, fmt.Errorf("mark run dead: %w", err)
		}
		return p.store.GetRun(context.Background(), run.ID)
	}
	retry := p.retryRun(job, run)
	if err := p.store.CreateRun(context.Background(), retry); err != nil {
		return run, fmt.Errorf("create retry run: %w", err)
	}
	p.logger.Info("retry scheduled", "run_id", retry.ID, "job_id", job.ID, "attempt", retry.Attempt, "scheduled_at", retry.ScheduledAt)
	return p.store.GetRun(context.Background(), run.ID)
}

func (p *Pool) retryRun(job store.Job, run store.Run) store.Run {
	p.rngMu.Lock()
	delay := BackoffDelay(job.Backoff, run.Attempt, p.rng)
	p.rngMu.Unlock()
	now := time.Now()
	return store.Run{
		ID:          newRunID(),
		JobID:       run.JobID,
		Status:      "pending",
		Attempt:     run.Attempt + 1,
		ScheduledAt: now.Add(delay),
		CreatedAt:   now,
	}
}

type logSequence struct {
	mu       sync.Mutex
	seq      int64
	store    store.Store
	runID    string
	errMu    sync.Mutex
	writeErr error
	lease    *runLease
}

func (s *logSequence) append(stream, chunk string) error {
	s.mu.Lock()
	s.seq++
	entry := store.Log{RunID: s.runID, Stream: stream, Chunk: chunk, Seq: s.seq, CreatedAt: time.Now()}
	var err error
	if s.lease != nil {
		if s.lease.lost.Load() {
			err = store.ErrLeaseLost
		} else {
			_, err = leaseCall(context.Background(), s.lease.ttl, func(ctx context.Context) (struct{}, error) {
				return struct{}{}, s.lease.store.AppendRunLog(ctx, s.lease.owner, entry)
			})
		}
		if err != nil {
			s.lease.lose(err)
		}
	} else {
		err = s.store.AppendLog(context.Background(), entry)
	}
	s.mu.Unlock()
	if err != nil {
		s.errMu.Lock()
		s.writeErr = err
		s.errMu.Unlock()
	}
	return err
}

func (s *logSequence) error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.writeErr
}

type chunkWriter struct {
	mu       sync.Mutex
	buffer   []byte
	stream   string
	sequence *logSequence
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = append(w.buffer, p...)
	for len(w.buffer) >= logChunkSize {
		if err := w.sequence.append(w.stream, string(w.buffer[:logChunkSize])); err != nil {
			return len(p), err
		}
		w.buffer = w.buffer[logChunkSize:]
	}
	return len(p), nil
}

func (w *chunkWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buffer) == 0 {
		return nil
	}
	err := w.sequence.append(w.stream, string(w.buffer))
	w.buffer = nil
	return err
}

func newRunID() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

type leaseResult[T any] struct {
	value T
	err   error
}

func leaseCall[T any](parent context.Context, ttl time.Duration, call func(context.Context) (T, error)) (T, error) {
	timeout := ttl / 6
	if timeout > time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	result := make(chan leaseResult[T], 1)
	go func() {
		value, err := call(ctx)
		result <- leaseResult[T]{value: value, err: err}
	}()
	select {
	case result := <-result:
		return result.value, result.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

type runLease struct {
	store    store.DistributedStore
	runID    string
	owner    string
	ttl      time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	stop     chan struct{}
	done     chan struct{}
	lost     atomic.Bool
	stopOnce sync.Once
	logger   *slog.Logger
}

func newRunLease(parent context.Context, st store.DistributedStore, runID string, ttl time.Duration, logger *slog.Logger) *runLease {
	ctx, cancel := context.WithCancel(parent)
	return &runLease{store: st, runID: runID, owner: newRunID(), ttl: ttl, ctx: ctx, cancel: cancel, stop: make(chan struct{}), logger: logger}
}

func (l *runLease) start() {
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		ticker := time.NewTicker(l.ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-ticker.C:
				owned, err := leaseCall(context.Background(), l.ttl, func(ctx context.Context) (bool, error) {
					return l.store.RenewRunLease(ctx, l.runID, l.owner, time.Now(), l.ttl)
				})
				if err != nil || !owned {
					if err == nil {
						err = store.ErrLeaseLost
					}
					l.lose(err)
					return
				}
			}
		}
	}()
}

func (l *runLease) lose(err error) {
	if l.lost.CompareAndSwap(false, true) {
		l.cancel()
		l.logger.Warn("run lease lost; execution canceled", "run_id", l.runID, "error", err)
	}
}

func (l *runLease) stopRenewal() {
	l.stopOnce.Do(func() { close(l.stop) })
}

func (l *runLease) close() {
	l.stopRenewal()
	if l.done != nil {
		<-l.done
	}
	l.cancel()
}

var _ io.Writer = (*chunkWriter)(nil)
