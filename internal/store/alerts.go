package store

import (
	"context"
	"time"
)

// Alert is a durable webhook outbox entry. It snapshots the run result at
// enqueue time so delivery does not depend on the job or run still existing.
type Alert struct {
	ID            string     `json:"ID"`
	RunID         string     `json:"RunID"`
	JobID         string     `json:"JobID"`
	JobName       string     `json:"JobName"`
	Status        string     `json:"Status"`
	Attempt       int        `json:"Attempt"`
	ExitCode      *int       `json:"ExitCode"`
	DurationMS    *int64     `json:"DurationMS"`
	Error         string     `json:"Error"`
	FinishedAt    *time.Time `json:"FinishedAt"`
	Attempts      int        `json:"Attempts"`
	NextAttemptAt time.Time  `json:"NextAttemptAt"`
	LeaseUntil    *time.Time `json:"LeaseUntil"`
	DeliveredAt   *time.Time `json:"DeliveredAt"`
	AbandonedAt   *time.Time `json:"AbandonedAt"`
	LastError     string     `json:"LastError"`
	CreatedAt     time.Time  `json:"CreatedAt"`
}

// AlertResult describes how a claimed alert delivery finished.
type AlertResult struct {
	Delivered bool
	RetryAt   *time.Time
	LastError string
}

// AlertStore is the durable outbox used by the webhook delivery worker.
type AlertStore interface {
	EnqueueAlert(context.Context, Alert) error
	ClaimAlerts(context.Context, string, time.Time, time.Duration, int) ([]Alert, error)
	CompleteAlert(context.Context, string, string, time.Time, AlertResult) error
	AlertCandidates(context.Context, time.Time, time.Time, []string, int) ([]Alert, error)
}

func normalizeAlert(a Alert, now time.Time) Alert {
	if a.ID == "" && a.RunID != "" && a.Status != "" {
		a.ID = a.RunID + ":" + a.Status
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	if a.NextAttemptAt.IsZero() {
		a.NextAttemptAt = a.CreatedAt
	}
	a.CreatedAt, a.NextAttemptAt = a.CreatedAt.UTC(), a.NextAttemptAt.UTC()
	if a.FinishedAt != nil {
		t := a.FinishedAt.UTC()
		a.FinishedAt = &t
	}
	if a.LeaseUntil != nil {
		t := a.LeaseUntil.UTC()
		a.LeaseUntil = &t
	}
	if a.DeliveredAt != nil {
		t := a.DeliveredAt.UTC()
		a.DeliveredAt = &t
	}
	if a.AbandonedAt != nil {
		t := a.AbandonedAt.UTC()
		a.AbandonedAt = &t
	}
	return a
}
