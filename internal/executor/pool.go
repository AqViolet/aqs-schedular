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
	"syscall"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

const logChunkSize = 1024

type Pool struct {
	store        store.Store
	logger       *slog.Logger
	concurrency  int
	poll         time.Duration
	stop         chan struct{}
	done         chan struct{}
	dispatchDone chan struct{}
	cancelExec   context.CancelFunc
	stopOnce     sync.Once
	rngMu        sync.Mutex
	rng          *mrand.Rand
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
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		rng:         mrand.New(mrand.NewSource(time.Now().UnixNano())),
	}
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
			runs, err := p.store.PendingRuns(context.Background(), p.concurrency*2)
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

func (p *Pool) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
	if p.dispatchDone != nil {
		<-p.dispatchDone
	}
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
	claimed, err := p.store.ClaimRun(ctx, run.ID, time.Now())
	if err != nil {
		p.logger.Error("claim pending run", "run_id", run.ID, "error", err)
		return
	}
	if !claimed {
		return
	}
	run, err = p.store.GetRun(ctx, run.ID)
	if err != nil {
		p.logger.Error("load claimed run", "run_id", run.ID, "error", err)
		return
	}
	job, err := p.store.GetJob(ctx, run.JobID)
	if err != nil {
		p.finish(run, "failed", nil, 0, fmt.Sprintf("load job: %v", err))
		return
	}
	p.execute(parent, job, run)
}

func (p *Pool) execute(parent context.Context, job store.Job, run store.Run) {
	started := time.Now()
	var ctx context.Context
	var cancel context.CancelFunc
	if job.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(parent, time.Duration(job.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	seq := &logSequence{store: p.store, runID: run.ID}
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
	if err := p.finish(run, status, exitCode, duration, message); err != nil {
		p.logger.Error("persist run result", "run_id", run.ID, "error", err)
		return
	}
	if status == "failed" {
		if parent.Err() != nil {
			p.logger.Warn("run abandoned during shutdown", "run_id", run.ID, "job_id", job.ID)
			return
		}
		p.retry(job, run)
		p.logger.Warn("run failed", "run_id", run.ID, "job_id", job.ID, "error", message)
		return
	}
	p.logger.Info("run completed", "run_id", run.ID, "job_id", job.ID, "duration_ms", duration)
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

func (p *Pool) retry(job store.Job, run store.Run) {
	var err error
	run, err = p.store.GetRun(context.Background(), run.ID)
	if err != nil {
		p.logger.Error("load failed run before retry", "run_id", run.ID, "error", err)
		return
	}
	if run.Attempt >= job.MaxRetries {
		run.Status = "dead"
		if err := p.store.UpdateRun(context.Background(), run); err != nil {
			p.logger.Error("mark run dead", "run_id", run.ID, "error", err)
		}
		return
	}
	p.rngMu.Lock()
	delay := BackoffDelay(job.Backoff, run.Attempt, p.rng)
	p.rngMu.Unlock()
	now := time.Now()
	retry := store.Run{
		ID:          newRunID(),
		JobID:       run.JobID,
		Status:      "pending",
		Attempt:     run.Attempt + 1,
		ScheduledAt: now.Add(delay),
		CreatedAt:   now,
	}
	if err := p.store.CreateRun(context.Background(), retry); err != nil {
		p.logger.Error("create retry run", "run_id", run.ID, "error", err)
		return
	}
	p.logger.Info("retry scheduled", "run_id", retry.ID, "job_id", job.ID, "attempt", retry.Attempt, "scheduled_at", retry.ScheduledAt)
}

type logSequence struct {
	mu       sync.Mutex
	seq      int64
	store    store.Store
	runID    string
	errMu    sync.Mutex
	writeErr error
}

func (s *logSequence) append(stream, chunk string) error {
	s.mu.Lock()
	s.seq++
	entry := store.Log{RunID: s.runID, Stream: stream, Chunk: chunk, Seq: s.seq, CreatedAt: time.Now()}
	err := s.store.AppendLog(context.Background(), entry)
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

var _ io.Writer = (*chunkWriter)(nil)
