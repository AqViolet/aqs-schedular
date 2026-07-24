package store

import (
	"context"
	"database/sql"

	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type SQLite struct {
	db *sql.DB
}

func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db}
	for _, q := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
	} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure sqlite: %w", err)
		}
	}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("create schema version: %w", err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_version`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_version(version) VALUES (0)`); err != nil {
			return err
		}
	}
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT version FROM schema_version LIMIT 1`).Scan(&version); err != nil {
		return err
	}
	if version > 2 {
		return fmt.Errorf("database schema version %d is newer than supported version 2", version)
	}
	if version < 1 {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, q := range []string{
			`CREATE TABLE jobs (
				id TEXT PRIMARY KEY,
				name TEXT NOT NULL,
				schedule TEXT NOT NULL,
				command TEXT NOT NULL,
				timeout_seconds INTEGER NOT NULL DEFAULT 0,
				max_retries INTEGER NOT NULL DEFAULT 0,
				backoff TEXT NOT NULL DEFAULT 'exponential',
				status TEXT NOT NULL,
				next_run_at INTEGER,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			)`,
			`CREATE TABLE runs (
				id TEXT PRIMARY KEY,
				job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
				status TEXT NOT NULL,
				attempt INTEGER NOT NULL DEFAULT 1,
				scheduled_at INTEGER NOT NULL,
				started_at INTEGER,
				finished_at INTEGER,
				exit_code INTEGER,
				duration_ms INTEGER,
				error TEXT,
				created_at INTEGER NOT NULL
			)`,
			`CREATE TABLE run_logs (
				run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
				stream TEXT NOT NULL,
				chunk TEXT NOT NULL,
				seq INTEGER NOT NULL,
				created_at INTEGER NOT NULL,
				PRIMARY KEY (run_id, seq)
			)`,
			`CREATE INDEX idx_jobs_due ON jobs(status, next_run_at)`,
			`CREATE INDEX idx_runs_job_scheduled ON runs(job_id, scheduled_at DESC)`,
			`UPDATE schema_version SET version = 1`,
		} {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("apply schema migration: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema migration: %w", err)
		}
	}
	if version < 2 {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, q := range []string{
			`CREATE TABLE alert_outbox (
				id TEXT PRIMARY KEY,
				run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
				job_id TEXT NOT NULL,
				job_name TEXT NOT NULL,
				status TEXT NOT NULL,
				attempt INTEGER NOT NULL,
				exit_code INTEGER,
				duration_ms INTEGER,
				error TEXT,
				finished_at INTEGER,
				attempts INTEGER NOT NULL DEFAULT 0,
				next_attempt_at INTEGER NOT NULL,
				lease_owner TEXT,
				lease_until INTEGER,
				delivered_at INTEGER,
				abandoned_at INTEGER,
				last_error TEXT,
				created_at INTEGER NOT NULL
			)`,
			`CREATE INDEX idx_alerts_due ON alert_outbox(delivered_at, abandoned_at, next_attempt_at)`,
			`CREATE INDEX idx_alerts_run ON alert_outbox(run_id)`,
			`UPDATE schema_version SET version = 2`,
		} {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("apply alert migration: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit alert migration: %w", err)
		}
	}
	return nil
}

const jobColumns = `id, name, schedule, command, timeout_seconds, max_retries, backoff, status, next_run_at, created_at, updated_at`

func (s *SQLite) CreateJob(ctx context.Context, j Job) error {
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	if j.UpdatedAt.IsZero() {
		j.UpdatedAt = j.CreatedAt
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs (`+jobColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Name, j.Schedule, j.Command, j.TimeoutSeconds, j.MaxRetries, j.Backoff, j.Status,
		unixPtr(j.NextRunAt), j.CreatedAt.Unix(), j.UpdatedAt.Unix())
	return err
}

func (s *SQLite) GetJob(ctx context.Context, id string) (Job, error) {
	return scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id))
}

func (s *SQLite) ListJobs(ctx context.Context, status string) ([]Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY name, id`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *SQLite) DueJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= ? ORDER BY next_run_at`, now.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *SQLite) UpdateJob(ctx context.Context, j Job) error {
	j.UpdatedAt = time.Now()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET name=?, schedule=?, command=?, timeout_seconds=?, max_retries=?, backoff=?, status=?, next_run_at=?, updated_at=? WHERE id=?`,
		j.Name, j.Schedule, j.Command, j.TimeoutSeconds, j.MaxRetries, j.Backoff, j.Status, unixPtr(j.NextRunAt), j.UpdatedAt.Unix(), j.ID)
	return requireRow(res, err)
}

func (s *SQLite) UpdateJobConfiguration(ctx context.Context, j Job, scheduleChanged bool) error {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET name=?, schedule=?, command=?, timeout_seconds=?, max_retries=?, backoff=?, next_run_at=CASE WHEN ? THEN ? ELSE next_run_at END, updated_at=? WHERE id=?`,
		j.Name, j.Schedule, j.Command, j.TimeoutSeconds, j.MaxRetries, j.Backoff, scheduleChanged, unixPtr(j.NextRunAt), time.Now().Unix(), j.ID)
	return requireRow(res, err)
}

func (s *SQLite) SetJobStatus(ctx context.Context, id, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?, updated_at=? WHERE id=?`, status, time.Now().Unix(), id)
	return requireRow(res, err)
}

func (s *SQLite) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id=?`, id)
	return requireRow(res, err)
}

const runColumns = `id, job_id, status, attempt, scheduled_at, started_at, finished_at, exit_code, duration_ms, error, created_at`

func (s *SQLite) CreateRun(ctx context.Context, r Run) error {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO runs (`+runColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.JobID, r.Status, r.Attempt, r.ScheduledAt.Unix(), timePtrUnix(r.StartedAt), timePtrUnix(r.FinishedAt), intPtr(r.ExitCode), int64Ptr(r.DurationMS), nullString(r.Error), r.CreatedAt.Unix())
	return err
}

func (s *SQLite) ScheduleJobRun(ctx context.Context, jobID string, r Run, next *time.Time) error {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO runs (`+runColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.JobID, r.Status, r.Attempt, r.ScheduledAt.Unix(), timePtrUnix(r.StartedAt), timePtrUnix(r.FinishedAt), intPtr(r.ExitCode), int64Ptr(r.DurationMS), nullString(r.Error), r.CreatedAt.Unix())
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET next_run_at=?, updated_at=? WHERE id=?`, unixPtr(next), time.Now().Unix(), jobID)
	if err := requireRow(res, err); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) GetRun(ctx context.Context, id string) (Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, id))
}

func (s *SQLite) PendingRuns(ctx context.Context, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM runs WHERE status='pending' AND scheduled_at <= ? ORDER BY scheduled_at, created_at LIMIT ?`, time.Now().Unix(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *SQLite) RunningRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM runs WHERE status='running' ORDER BY started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *SQLite) ClaimRun(ctx context.Context, id string, started time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status='running', started_at=? WHERE id=? AND status='pending' AND scheduled_at <= ?`, started.Unix(), id, started.Unix())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *SQLite) ListRuns(ctx context.Context, jobID string) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM runs WHERE job_id=? ORDER BY scheduled_at DESC, created_at DESC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *SQLite) UpdateRun(ctx context.Context, r Run) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status=?, attempt=?, scheduled_at=?, started_at=?, finished_at=?, exit_code=?, duration_ms=?, error=? WHERE id=?`,
		r.Status, r.Attempt, r.ScheduledAt.Unix(), timePtrUnix(r.StartedAt), timePtrUnix(r.FinishedAt), intPtr(r.ExitCode), int64Ptr(r.DurationMS), nullString(r.Error), r.ID)
	return requireRow(res, err)
}

func (s *SQLite) AppendLog(ctx context.Context, l Log) error {
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO run_logs(run_id, stream, chunk, seq, created_at) VALUES (?, ?, ?, ?, ?)`, l.RunID, l.Stream, l.Chunk, l.Seq, l.CreatedAt.Unix())
	return err
}

func (s *SQLite) ListLogs(ctx context.Context, runID string) ([]Log, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT run_id, stream, chunk, seq, created_at FROM run_logs WHERE run_id=? ORDER BY seq`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logs []Log
	for rows.Next() {
		var l Log
		var created int64
		if err := rows.Scan(&l.RunID, &l.Stream, &l.Chunk, &l.Seq, &created); err != nil {
			return nil, err
		}
		l.CreatedAt = time.Unix(created, 0)
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var j Job
	var next sql.NullInt64
	var created, updated int64
	err := row.Scan(&j.ID, &j.Name, &j.Schedule, &j.Command, &j.TimeoutSeconds, &j.MaxRetries, &j.Backoff, &j.Status, &next, &created, &updated)
	if err != nil {
		return Job{}, err
	}
	if next.Valid {
		t := time.Unix(next.Int64, 0)
		j.NextRunAt = &t
	}
	j.CreatedAt, j.UpdatedAt = time.Unix(created, 0), time.Unix(updated, 0)
	return j, nil
}

func scanRun(row scanner) (Run, error) {
	var r Run
	var scheduled, created int64
	var started, finished, exit, duration sql.NullInt64
	var errText sql.NullString
	err := row.Scan(&r.ID, &r.JobID, &r.Status, &r.Attempt, &scheduled, &started, &finished, &exit, &duration, &errText, &created)
	if err != nil {
		return Run{}, err
	}
	r.ScheduledAt, r.CreatedAt = time.Unix(scheduled, 0), time.Unix(created, 0)
	if started.Valid {
		t := time.Unix(started.Int64, 0)
		r.StartedAt = &t
	}
	if finished.Valid {
		t := time.Unix(finished.Int64, 0)
		r.FinishedAt = &t
	}
	if exit.Valid {
		v := int(exit.Int64)
		r.ExitCode = &v
	}
	if duration.Valid {
		v := duration.Int64
		r.DurationMS = &v
	}
	if errText.Valid {
		r.Error = errText.String
	}
	return r, nil
}

func unixPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Unix()
}

func timePtrUnix(t *time.Time) any { return unixPtr(t) }
func intPtr(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}
func int64Ptr(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}
func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func requireRow(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

var _ Store = (*SQLite)(nil)
var _ JobConfigurationStore = (*SQLite)(nil)
