package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/cron"
	"github.com/AqViolet/aqs-schedular/internal/store"
)

type Scheduler struct {
	store    store.Store
	logger   *slog.Logger
	interval time.Duration
	now      func() time.Time
}

func New(st store.Store, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{store: st, logger: logger, interval: time.Second, now: time.Now}
}

func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.Recover(ctx); err != nil {
		return err
	}
	if err := s.Tick(ctx); err != nil {
		s.logger.Error("scheduler tick failed", "error", err)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				s.logger.Error("scheduler tick failed", "error", err)
			}
		}
	}
}

func (s *Scheduler) Recover(ctx context.Context) error {
	now := s.now()
	jobs, err := s.store.DueJobs(ctx, now)
	if err != nil {
		return fmt.Errorf("load overdue jobs: %w", err)
	}
	for _, job := range jobs {
		if job.NextRunAt == nil {
			continue
		}
		schedule, err := cron.Parse(job.Schedule)
		if err != nil {
			s.logger.Error("invalid job schedule", "job_id", job.ID, "error", err)
			continue
		}
		if !missed(now, *job.NextRunAt, schedule) {
			continue
		}
		next := schedule.NextAfter(now)
		var nextAt *time.Time
		if !next.IsZero() {
			nextAt = &next
		}
		run := store.Run{
			ID:          newID(),
			JobID:       job.ID,
			Status:      "missed",
			Attempt:     1,
			ScheduledAt: *job.NextRunAt,
			CreatedAt:   now,
		}
		if err := s.store.ScheduleJobRun(ctx, job.ID, run, nextAt); err != nil {
			return fmt.Errorf("record missed run for job %s: %w", job.ID, err)
		}
		s.logger.Warn("recorded missed run", "job_id", job.ID, "run_id", run.ID, "scheduled_at", run.ScheduledAt)
	}
	return nil
}

func (s *Scheduler) Tick(ctx context.Context) error {
	now := s.now()
	jobs, err := s.store.DueJobs(ctx, now)
	if err != nil {
		return fmt.Errorf("load due jobs: %w", err)
	}
	for _, job := range jobs {
		if job.NextRunAt == nil {
			continue
		}
		schedule, err := cron.Parse(job.Schedule)
		if err != nil {
			s.logger.Error("invalid job schedule", "job_id", job.ID, "error", err)
			continue
		}
		next := schedule.NextAfter(now)
		var nextAt *time.Time
		if !next.IsZero() {
			nextAt = &next
		}
		run := store.Run{
			ID:          newID(),
			JobID:       job.ID,
			Status:      "pending",
			Attempt:     1,
			ScheduledAt: *job.NextRunAt,
			CreatedAt:   now,
		}
		if err := s.store.ScheduleJobRun(ctx, job.ID, run, nextAt); err != nil {
			return fmt.Errorf("schedule job %s: %w", job.ID, err)
		}
		s.logger.Info("scheduled run", "job_id", job.ID, "run_id", run.ID, "scheduled_at", run.ScheduledAt)
	}
	return nil
}

func missed(now, scheduled time.Time, schedule cron.Schedule) bool {
	next := schedule.NextAfter(scheduled)
	if next.IsZero() {
		return now.After(scheduled)
	}
	return now.Sub(scheduled) > next.Sub(scheduled)
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
