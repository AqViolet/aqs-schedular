package api

import (
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

type jobDTO struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Schedule       string  `json:"schedule"`
	Command        string  `json:"command"`
	TimeoutSeconds int     `json:"timeout_seconds"`
	MaxRetries     int     `json:"max_retries"`
	Backoff        string  `json:"backoff"`
	Status         string  `json:"status"`
	NextRunAt      *string `json:"next_run_at"`
	CreatedAt      *string `json:"created_at"`
	UpdatedAt      *string `json:"updated_at"`
}

type runDTO struct {
	ID          string  `json:"id"`
	JobID       string  `json:"job_id"`
	Status      string  `json:"status"`
	Attempt     int     `json:"attempt"`
	ScheduledAt *string `json:"scheduled_at"`
	StartedAt   *string `json:"started_at"`
	FinishedAt  *string `json:"finished_at"`
	ExitCode    *int    `json:"exit_code"`
	DurationMS  *int64  `json:"duration_ms"`
	Error       string  `json:"error"`
	CreatedAt   *string `json:"created_at"`
}

type logDTO struct {
	RunID     string  `json:"run_id"`
	Stream    string  `json:"stream"`
	Chunk     string  `json:"chunk"`
	Seq       int64   `json:"seq"`
	CreatedAt *string `json:"created_at"`
}

func timestamp(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	value := t.UTC().Format(time.RFC3339)
	return &value
}

func optionalTimestamp(t *time.Time) *string {
	if t == nil {
		return nil
	}
	return timestamp(*t)
}

func jobResponse(job store.Job) jobDTO {
	return jobDTO{
		ID: job.ID, Name: job.Name, Schedule: job.Schedule, Command: job.Command,
		TimeoutSeconds: job.TimeoutSeconds, MaxRetries: job.MaxRetries, Backoff: job.Backoff,
		Status: job.Status, NextRunAt: optionalTimestamp(job.NextRunAt),
		CreatedAt: timestamp(job.CreatedAt), UpdatedAt: timestamp(job.UpdatedAt),
	}
}

func runResponse(run store.Run) runDTO {
	return runDTO{
		ID: run.ID, JobID: run.JobID, Status: run.Status, Attempt: run.Attempt,
		ScheduledAt: timestamp(run.ScheduledAt), StartedAt: optionalTimestamp(run.StartedAt),
		FinishedAt: optionalTimestamp(run.FinishedAt), ExitCode: run.ExitCode,
		DurationMS: run.DurationMS, Error: run.Error, CreatedAt: timestamp(run.CreatedAt),
	}
}
