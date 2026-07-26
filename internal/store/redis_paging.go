package store

import (
	"context"
	"errors"
)

func (s *Redis) ListJobsPage(ctx context.Context, status string, limit, offset int) ([]Job, error) {
	if limit <= 0 || offset < 0 {
		return nil, errors.New("job page requires a positive limit and nonnegative offset")
	}
	jobs, err := s.ListJobs(ctx, status)
	if err != nil {
		return nil, err
	}
	if offset >= len(jobs) {
		return nil, nil
	}
	jobs = jobs[offset:]
	if len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (s *Redis) ListRunsPage(ctx context.Context, jobID, status string, limit, offset int) ([]Run, error) {
	if limit <= 0 || offset < 0 {
		return nil, errors.New("run page requires a positive limit and nonnegative offset")
	}
	runs, err := s.ListRuns(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if status != "" {
		filtered := runs[:0]
		for _, run := range runs {
			if run.Status == status {
				filtered = append(filtered, run)
			}
		}
		runs = filtered
	}
	if offset >= len(runs) {
		return nil, nil
	}
	runs = runs[offset:]
	if len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

func (s *Redis) ListLogsPage(ctx context.Context, runID string, limit int, before, after int64) ([]Log, error) {
	if limit <= 0 {
		return nil, errors.New("log page requires a positive limit")
	}
	if before > 0 && after > 0 {
		return nil, errors.New("log page accepts before or after, not both")
	}
	logs, err := s.ListLogs(ctx, runID)
	if err != nil {
		return nil, err
	}
	filtered := logs[:0]
	for _, entry := range logs {
		if after > 0 && entry.Seq <= after {
			continue
		}
		if before > 0 && entry.Seq >= before {
			continue
		}
		filtered = append(filtered, entry)
	}
	if after > 0 {
		if len(filtered) > limit {
			filtered = filtered[:limit]
		}
		return filtered, nil
	}
	if len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered, nil
}

var (
	_ JobPageStore = (*Redis)(nil)
	_ RunPageStore = (*Redis)(nil)
	_ LogPageStore = (*Redis)(nil)
)
