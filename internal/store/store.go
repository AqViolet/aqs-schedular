package store

import (
	"context"
	"errors"
	"time"
)

type Job struct {
	ID             string
	Name           string
	Schedule       string
	Command        string
	TimeoutSeconds int
	MaxRetries     int
	Backoff        string
	Status         string
	NextRunAt      *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Run struct {
	ID          string
	JobID       string
	Status      string
	Attempt     int
	ScheduledAt time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
	ExitCode    *int
	DurationMS  *int64
	Error       string
	CreatedAt   time.Time
}

type Log struct {
	RunID     string
	Stream    string
	Chunk     string
	Seq       int64
	CreatedAt time.Time
}

var ErrLeaseLost = errors.New("run lease lost")

type JobConfigurationStore interface {
	UpdateJobConfiguration(context.Context, Job, bool) error
}

type DistributedStore interface {
	Store
	ClaimRunLease(context.Context, string, string, time.Time, time.Duration) (bool, error)
	RenewRunLease(context.Context, string, string, time.Time, time.Duration) (bool, error)
	CompleteRun(context.Context, Run, string, *Run) error
	AppendRunLog(context.Context, string, Log) error
	RecoverExpiredRuns(context.Context, time.Time) error
}

type Store interface {
	Close() error
	CreateJob(context.Context, Job) error
	GetJob(context.Context, string) (Job, error)
	ListJobs(context.Context, string) ([]Job, error)
	DueJobs(context.Context, time.Time) ([]Job, error)
	UpdateJob(context.Context, Job) error
	SetJobStatus(context.Context, string, string) error
	DeleteJob(context.Context, string) error
	CreateRun(context.Context, Run) error
	ScheduleJobRun(context.Context, string, Run, *time.Time) error
	GetRun(context.Context, string) (Run, error)
	PendingRuns(context.Context, int) ([]Run, error)
	RunningRuns(context.Context) ([]Run, error)
	ClaimRun(context.Context, string, time.Time) (bool, error)
	ListRuns(context.Context, string) ([]Run, error)
	UpdateRun(context.Context, Run) error
	AppendLog(context.Context, Log) error
	ListLogs(context.Context, string) ([]Log, error)
}
