package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/config"
	"github.com/AqViolet/aqs-schedular/internal/store"
)

type migrationCounts struct {
	jobs, jobsSkipped int
	runs, runsSkipped int
	logs, logsSkipped int
}

func migrateCommand(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	from := fs.String("from", "sqlite", "source backend (only sqlite)")
	dbPath := fs.String("db", config.DatabasePath(), "source SQLite database")
	redisURL := fs.String("redis-url", os.Getenv("AQS_REDIS_URL"), "target Redis URL")
	prefix := fs.String("prefix", defaultRedisPrefix(), "target Redis key prefix")
	dryRun := fs.Bool("dry-run", false, "count records without writing to Redis")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("migrate does not accept positional arguments")
	}
	if *from != "sqlite" {
		return fmt.Errorf("unsupported source backend %q; only sqlite is supported", *from)
	}
	if strings.TrimSpace(*redisURL) == "" {
		return errors.New("migrate requires --redis-url or AQS_REDIS_URL")
	}
	source, err := store.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open SQLite source: %w", err)
	}
	defer source.Close()
	target, err := store.OpenRedis(*redisURL, *prefix)
	if err != nil {
		return fmt.Errorf("open Redis target: %w", err)
	}
	defer target.Close()
	counts, err := migrateStore(context.Background(), source, target, *dryRun)
	if err != nil {
		return err
	}
	verb := "copied"
	if *dryRun {
		verb = "would copy"
	}
	fmt.Fprintf(stdout, "%s %d jobs (%d already present), %d runs (%d already present), %d logs (%d already present)\n",
		verb, counts.jobs, counts.jobsSkipped, counts.runs, counts.runsSkipped, counts.logs, counts.logsSkipped)
	return nil
}

func defaultRedisPrefix() string {
	if prefix := os.Getenv("AQS_REDIS_PREFIX"); prefix != "" {
		return prefix
	}
	return "aqs-scheduler"
}

func migrateStore(ctx context.Context, source, target store.Store, dryRun bool) (migrationCounts, error) {
	var counts migrationCounts
	jobs, err := source.ListJobs(ctx, "")
	if err != nil {
		return counts, fmt.Errorf("list source jobs: %w", err)
	}
	for _, job := range jobs {
		if _, err := target.GetJob(ctx, job.ID); err == nil {
			counts.jobsSkipped++
		} else if !errors.Is(err, sql.ErrNoRows) {
			return counts, fmt.Errorf("read target job %s: %w", job.ID, err)
		} else {
			counts.jobs++
			if !dryRun {
				if err := target.CreateJob(ctx, job); err != nil {
					return counts, fmt.Errorf("copy job %s: %w", job.ID, err)
				}
			}
		}
		runs, err := source.ListRuns(ctx, job.ID)
		if err != nil {
			return counts, fmt.Errorf("list source runs for %s: %w", job.ID, err)
		}
		for _, run := range runs {
			targetRun, targetErr := target.GetRun(ctx, run.ID)
			if targetErr == nil {
				counts.runsSkipped++
			} else if !errors.Is(targetErr, sql.ErrNoRows) {
				return counts, fmt.Errorf("read target run %s: %w", run.ID, targetErr)
			} else {
				counts.runs++
				if !dryRun {
					if err := target.CreateRun(ctx, migrateRun(run)); err != nil {
						return counts, fmt.Errorf("copy run %s: %w", run.ID, err)
					}
				}
				targetRun = migrateRun(run)
			}
			logs, err := source.ListLogs(ctx, run.ID)
			if err != nil {
				return counts, fmt.Errorf("list source logs for %s: %w", run.ID, err)
			}
			existing := make(map[int64]bool)
			if !dryRun {
				targetLogs, err := target.ListLogs(ctx, targetRun.ID)
				if err != nil {
					return counts, fmt.Errorf("list target logs for %s: %w", run.ID, err)
				}
				for _, entry := range targetLogs {
					existing[entry.Seq] = true
				}
			}
			for _, entry := range logs {
				if existing[entry.Seq] {
					counts.logsSkipped++
					continue
				}
				counts.logs++
				if !dryRun {
					if err := target.AppendLog(ctx, entry); err != nil {
						return counts, fmt.Errorf("copy log %s/%d: %w", run.ID, entry.Seq, err)
					}
				}
			}
		}
	}
	return counts, nil
}

// migrateRun converts a mid-flight run into queued work because the source
// worker's lease does not exist in the target store.
func migrateRun(run store.Run) store.Run {
	if run.Status != "running" {
		return run
	}
	run.Status = "pending"
	run.Attempt++
	run.ScheduledAt = time.Now().UTC()
	run.StartedAt, run.FinishedAt, run.ExitCode, run.DurationMS = nil, nil, nil, nil
	run.Error = "migrated: interrupted run requeued"
	return run
}
