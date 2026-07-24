package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *SQLite) PruneRuns(ctx context.Context, before time.Time, statuses []string, dryRun bool) (int64, error) {
	statuses, err := ValidatePruneStatuses(statuses)
	if err != nil {
		return 0, err
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, 0, len(statuses)+1)
	for i, status := range statuses {
		placeholders[i] = "?"
		args = append(args, status)
	}
	args = append(args, before.UTC().Unix())
	where := `status IN (` + strings.Join(placeholders, ",") + `) AND scheduled_at < ?
		AND NOT EXISTS (SELECT 1 FROM alert_outbox a WHERE a.run_id = runs.id AND a.delivered_at IS NULL AND a.abandoned_at IS NULL)`
	if dryRun {
		var count int64
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE `+where, args...).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE `+where, args...)
	if err != nil {
		return 0, fmt.Errorf("prune runs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

var _ RetentionStore = (*SQLite)(nil)
