package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var prunableRunStatuses = map[string]bool{"succeeded": true, "failed": true, "dead": true, "missed": true}

// RetentionStore deletes terminal run history older than a cutoff while
// preserving runs with undelivered alerts.
type RetentionStore interface {
	PruneRuns(context.Context, time.Time, []string, bool) (int64, error)
}

// ValidatePruneStatuses rejects live states and unknown states so retention
// can never delete queued or running work.
func ValidatePruneStatuses(statuses []string) ([]string, error) {
	if len(statuses) == 0 {
		statuses = []string{"succeeded", "failed", "dead", "missed"}
	}
	seen := make(map[string]bool, len(statuses))
	out := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if !prunableRunStatuses[status] {
			return nil, fmt.Errorf("run status %q cannot be pruned; allowed: succeeded, failed, dead, missed", status)
		}
		if !seen[status] {
			seen[status] = true
			out = append(out, status)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one run status is required")
	}
	return out, nil
}
