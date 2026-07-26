package store

import (
	"context"
	"errors"
	"time"
)

func (s *SQLite) ListJobsPage(ctx context.Context, status string, limit, offset int) ([]Job, error) {
	if limit <= 0 || offset < 0 {
		return nil, errors.New("job page requires a positive limit and nonnegative offset")
	}
	q := `SELECT ` + jobColumns + ` FROM jobs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY name, id LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
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

func (s *SQLite) ListRunsPage(ctx context.Context, jobID, status string, limit, offset int) ([]Run, error) {
	if limit <= 0 || offset < 0 {
		return nil, errors.New("run page requires a positive limit and nonnegative offset")
	}
	q := `SELECT ` + runColumns + ` FROM runs WHERE job_id=?`
	args := []any{jobID}
	if status != "" {
		q += ` AND status=?`
		args = append(args, status)
	}
	q += ` ORDER BY scheduled_at DESC, created_at DESC, id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
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

func (s *SQLite) ListLogsPage(ctx context.Context, runID string, limit int, before, after int64) ([]Log, error) {
	if limit <= 0 {
		return nil, errors.New("log page requires a positive limit")
	}
	if before > 0 && after > 0 {
		return nil, errors.New("log page accepts before or after, not both")
	}
	var query string
	var args []any
	if after > 0 {
		query = `SELECT run_id, stream, chunk, seq, created_at FROM run_logs WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?`
		args = []any{runID, after, limit}
	} else {
		query = `SELECT run_id, stream, chunk, seq, created_at FROM (
			SELECT run_id, stream, chunk, seq, created_at FROM run_logs WHERE run_id=?`
		args = []any{runID}
		if before > 0 {
			query += ` AND seq<?`
			args = append(args, before)
		}
		query += ` ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
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

var (
	_ JobPageStore = (*SQLite)(nil)
	_ RunPageStore = (*SQLite)(nil)
	_ LogPageStore = (*SQLite)(nil)
)
