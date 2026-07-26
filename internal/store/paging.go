package store

import "context"

// JobPageStore optionally serves jobs with limit/offset instead of loading
// the whole collection.
type JobPageStore interface {
	ListJobsPage(context.Context, string, int, int) ([]Job, error)
}

// RunPageStore optionally serves a job's runs with limit/offset.
type RunPageStore interface {
	ListRunsPage(context.Context, string, string, int, int) ([]Run, error)
}

// LogPageStore serves bounded log windows. Limit is always positive. When
// After > 0, return the first chunks with seq > After in ascending order.
// When Before > 0, return the last chunks with seq < Before in ascending
// order. With neither, return the last chunks for the run.
type LogPageStore interface {
	ListLogsPage(context.Context, string, int, int64, int64) ([]Log, error)
}
