package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

const alertColumns = `id, run_id, job_id, job_name, status, attempt, exit_code, duration_ms, error, finished_at, attempts, next_attempt_at, lease_until, delivered_at, abandoned_at, last_error, created_at`

func (s *SQLite) EnqueueAlert(ctx context.Context, a Alert) error {
	a = normalizeAlert(a, time.Now())
	_, err := s.db.ExecContext(ctx, `INSERT INTO alert_outbox (`+alertColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		a.ID, a.RunID, a.JobID, a.JobName, a.Status, a.Attempt, intPtr(a.ExitCode), int64Ptr(a.DurationMS), nullString(a.Error),
		unixPtr(a.FinishedAt), a.Attempts, a.NextAttemptAt.Unix(), unixPtr(a.LeaseUntil), unixPtr(a.DeliveredAt), unixPtr(a.AbandonedAt), nullString(a.LastError), a.CreatedAt.Unix())
	return err
}

func (s *SQLite) ClaimAlerts(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Alert, error) {
	if owner == "" || lease < time.Millisecond {
		return nil, errors.New("alert claim requires an owner and lease of at least one millisecond")
	}
	if limit <= 0 {
		limit = 20
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+alertColumns+` FROM alert_outbox
		WHERE delivered_at IS NULL AND abandoned_at IS NULL AND next_attempt_at <= ?
		  AND (lease_until IS NULL OR lease_until <= ?)
		ORDER BY next_attempt_at, created_at LIMIT ?`, now.Unix(), now.Unix(), limit)
	if err != nil {
		return nil, err
	}
	var alerts []Alert
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		alerts = append(alerts, a)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	leaseUntil := now.Add(lease).UTC()
	for i := range alerts {
		alerts[i].Attempts++
		alerts[i].LeaseUntil = &leaseUntil
		if _, err := tx.ExecContext(ctx, `UPDATE alert_outbox SET attempts=?, lease_owner=?, lease_until=? WHERE id=?`,
			alerts[i].Attempts, owner, leaseUntil.Unix(), alerts[i].ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return alerts, nil
}

func (s *SQLite) CompleteAlert(ctx context.Context, id, owner string, now time.Time, result AlertResult) error {
	if id == "" || owner == "" {
		return errors.New("alert completion requires an ID and owner")
	}
	now = now.UTC()
	var res sql.Result
	var err error
	switch {
	case result.Delivered:
		res, err = s.db.ExecContext(ctx, `UPDATE alert_outbox SET delivered_at=?, lease_owner=NULL, lease_until=NULL, last_error=? WHERE id=? AND lease_owner=?`,
			now.Unix(), nullString(result.LastError), id, owner)
	case result.RetryAt != nil:
		res, err = s.db.ExecContext(ctx, `UPDATE alert_outbox SET next_attempt_at=?, lease_owner=NULL, lease_until=NULL, last_error=? WHERE id=? AND lease_owner=?`,
			result.RetryAt.UTC().Unix(), nullString(result.LastError), id, owner)
	default:
		res, err = s.db.ExecContext(ctx, `UPDATE alert_outbox SET abandoned_at=?, lease_owner=NULL, lease_until=NULL, last_error=? WHERE id=? AND lease_owner=?`,
			now.Unix(), nullString(result.LastError), id, owner)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *SQLite) AlertCandidates(ctx context.Context, now, since time.Time, statuses []string, limit int) ([]Alert, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, 0, len(statuses)+2)
	for i, status := range statuses {
		placeholders[i] = "?"
		args = append(args, status)
	}
	args = append(args, since.Unix(), limit)
	rows, err := s.db.QueryContext(ctx, `SELECT r.id, r.job_id, j.name, r.status, r.attempt, r.exit_code, r.duration_ms, r.error, r.finished_at, r.created_at
		FROM runs r JOIN jobs j ON j.id = r.job_id
		LEFT JOIN alert_outbox a ON a.id = r.id || ':' || r.status
		WHERE r.status IN (`+strings.Join(placeholders, ",")+`) AND r.finished_at IS NOT NULL AND r.finished_at >= ? AND a.id IS NULL
		ORDER BY r.finished_at, r.id LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var alerts []Alert
	for rows.Next() {
		var (
			a                 Alert
			exit, duration    sql.NullInt64
			finished, created sql.NullInt64
			errText           sql.NullString
		)
		if err := rows.Scan(&a.RunID, &a.JobID, &a.JobName, &a.Status, &a.Attempt, &exit, &duration, &errText, &finished, &created); err != nil {
			return nil, err
		}
		a.ID = a.RunID + ":" + a.Status
		a.NextAttemptAt = now.UTC()
		a.CreatedAt = now.UTC()
		if exit.Valid {
			v := int(exit.Int64)
			a.ExitCode = &v
		}
		if duration.Valid {
			v := duration.Int64
			a.DurationMS = &v
		}
		if errText.Valid {
			a.Error = errText.String
		}
		if finished.Valid {
			t := time.Unix(finished.Int64, 0).UTC()
			a.FinishedAt = &t
		}
		if created.Valid {
			a.CreatedAt = time.Unix(created.Int64, 0).UTC()
		}
		alerts = append(alerts, a)
	}
	return alerts, rows.Err()
}

func scanAlert(row scanner) (Alert, error) {
	var (
		a                                                 Alert
		exit, duration, next, lease, delivered, abandoned sql.NullInt64
		finished, created                                 sql.NullInt64
		errText, lastError                                sql.NullString
	)
	err := row.Scan(&a.ID, &a.RunID, &a.JobID, &a.JobName, &a.Status, &a.Attempt, &exit, &duration, &errText, &finished,
		&a.Attempts, &next, &lease, &delivered, &abandoned, &lastError, &created)
	if err != nil {
		return Alert{}, err
	}
	if exit.Valid {
		v := int(exit.Int64)
		a.ExitCode = &v
	}
	if duration.Valid {
		v := duration.Int64
		a.DurationMS = &v
	}
	if errText.Valid {
		a.Error = errText.String
	}
	if finished.Valid {
		t := time.Unix(finished.Int64, 0).UTC()
		a.FinishedAt = &t
	}
	if next.Valid {
		a.NextAttemptAt = time.Unix(next.Int64, 0).UTC()
	}
	if lease.Valid {
		t := time.Unix(lease.Int64, 0).UTC()
		a.LeaseUntil = &t
	}
	if delivered.Valid {
		t := time.Unix(delivered.Int64, 0).UTC()
		a.DeliveredAt = &t
	}
	if abandoned.Valid {
		t := time.Unix(abandoned.Int64, 0).UTC()
		a.AbandonedAt = &t
	}
	if lastError.Valid {
		a.LastError = lastError.String
	}
	a.CreatedAt = time.Unix(created.Int64, 0).UTC()
	return a, nil
}

var _ AlertStore = (*SQLite)(nil)
