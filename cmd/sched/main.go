package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/config"
	"github.com/AqViolet/aqs-schedular/internal/cron"
	"github.com/AqViolet/aqs-schedular/internal/store"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, nil)))
	if len(args) == 0 {
		return usageError()
	}
	if args[0] == "migrate" {
		return migrateCommand(args[1:], stdout, stderr)
	}
	settings, err := config.Load()
	if err != nil {
		return err
	}
	switch args[0] {
	case "daemon", "scheduler", "worker", "serve":
		return runService(args[0], args[1:], settings, stderr)
	}
	st, err := settings.OpenStore()
	if err != nil {
		return err
	}
	defer st.Close()
	ctx := context.Background()

	switch args[0] {
	case "add":
		return addJob(ctx, st, args[1:], stdout, stderr)
	case "list":
		return listJobs(ctx, st, args[1:], stdout, stderr)
	case "logs":
		return showLogs(ctx, st, args[1:], stdout, stderr)
	case "run":
		return triggerRun(ctx, st, args[1:], stdout)
	case "retry":
		return retryRun(ctx, st, args[1:], stdout)
	case "pause":
		return setStatus(ctx, st, args[1:], "paused", stdout)
	case "resume":
		return setStatus(ctx, st, args[1:], "active", stdout)
	case "delete":
		return deleteJob(ctx, st, args[1:], stdout)
	case "prune":
		return pruneRuns(ctx, st, args[1:], stdout, stderr)
	case "backup":
		return backupStore(ctx, st, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usageError() error {
	return errors.New("usage: sched <daemon|scheduler|worker|serve|add|list|logs|run|retry|pause|resume|delete|prune|backup|migrate>")
}

func addJob(ctx context.Context, st store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("add", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scheduleExpr := fs.String("schedule", "", "cron expression or supported macro")
	atExpr := fs.String("at", "", "one-shot RFC3339 time")
	name := fs.String("name", "", "job name")
	timeout := fs.Int("timeout", 0, "timeout in seconds")
	maxRetries := fs.Int("max-retries", 0, "maximum attempt number")
	backoff := fs.String("backoff", "exponential", "retry backoff: exponential, linear, none")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("add requires --name")
	}
	command := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if command == "" {
		return errors.New("add requires a command after --")
	}
	if *timeout < 0 || *maxRetries < 0 {
		return errors.New("timeout and max-retries cannot be negative")
	}
	if *backoff != "exponential" && *backoff != "linear" && *backoff != "none" {
		return fmt.Errorf("invalid backoff %q", *backoff)
	}
	if (*scheduleExpr == "") == (*atExpr == "") {
		return errors.New("specify exactly one of --schedule or --at")
	}
	expr := *scheduleExpr
	if *atExpr != "" {
		at, err := time.Parse(time.RFC3339, *atExpr)
		if err != nil {
			return fmt.Errorf("invalid --at time: %w", err)
		}
		expr = "once:" + at.Format(time.RFC3339)
	}
	schedule, err := cron.Parse(expr)
	if err != nil {
		return err
	}
	now := time.Now()
	next := schedule.NextAfter(now)
	if once, ok := schedule.(cron.Once); ok && next.IsZero() {
		next = once.At()
	}
	var nextAt *time.Time
	if !next.IsZero() {
		nextAt = &next
	}
	job := store.Job{
		ID:             newID(),
		Name:           *name,
		Schedule:       expr,
		Command:        command,
		TimeoutSeconds: *timeout,
		MaxRetries:     *maxRetries,
		Backoff:        *backoff,
		Status:         "active",
		NextRunAt:      nextAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		return err
	}
	fmt.Fprintln(stdout, job.ID)
	return nil
}

func listJobs(ctx context.Context, st store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	status := fs.String("status", "", "filter by active, paused, or dead")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("list does not accept positional arguments")
	}
	if *status != "" && *status != "active" && *status != "paused" && *status != "dead" {
		return fmt.Errorf("invalid status %q", *status)
	}
	jobs, err := st.ListJobs(ctx, *status)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSCHEDULE\tNEXT RUN\tCOMMAND")
	for _, job := range jobs {
		next := "-"
		if job.NextRunAt != nil {
			next = job.NextRunAt.Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", job.ID, job.Name, job.Status, job.Schedule, next, job.Command)
	}
	return w.Flush()
}

func showLogs(ctx context.Context, st store.Store, args []string, stdout, stderr io.Writer) error {
	flagArgs, positional, err := parseLogArgs(args)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runID := fs.String("run", "", "run ID")
	follow := fs.Bool("follow", false, "follow new log output")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New("usage: sched logs <job-id> [--run <run-id>] [--follow]")
	}
	jobID := positional[0]
	if _, err := st.GetJob(ctx, jobID); err != nil {
		return err
	}
	if *runID != "" {
		r, err := st.GetRun(ctx, *runID)
		if err != nil {
			return err
		}
		if r.JobID != jobID {
			return errors.New("run does not belong to the specified job")
		}
	}
	if !*follow {
		return printLogs(ctx, st, jobID, *runID, 0, stdout)
	}
	followCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var currentRun string
	var lastSeq int64
	if *runID != "" {
		currentRun = *runID
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if *runID == "" {
			runs, err := st.ListRuns(followCtx, jobID)
			if err != nil {
				return err
			}
			if len(runs) > 0 && runs[0].ID != currentRun {
				currentRun = runs[0].ID
				lastSeq = 0
			}
		}
		if currentRun != "" {
			logs, err := st.ListLogs(followCtx, currentRun)
			if err != nil {
				return err
			}
			for _, l := range logs {
				if l.Seq <= lastSeq {
					continue
				}
				if _, err := fmt.Fprintf(stdout, "[%s] %s", l.Stream, l.Chunk); err != nil {
					return err
				}
				lastSeq = l.Seq
			}
		}
		select {
		case <-followCtx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func parseLogArgs(args []string) ([]string, []string, error) {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--run":
			if i+1 >= len(args) {
				return nil, nil, errors.New("--run requires a run ID")
			}
			flags = append(flags, args[i], args[i+1])
			i++
		case strings.HasPrefix(args[i], "--run=") || args[i] == "--follow":
			flags = append(flags, args[i])
		default:
			positional = append(positional, args[i])
		}
	}
	return flags, positional, nil
}

func printLogs(ctx context.Context, st store.Store, jobID, runID string, after int64, stdout io.Writer) error {
	if runID == "" {
		runs, err := st.ListRuns(ctx, jobID)
		if err != nil {
			return err
		}
		if len(runs) == 0 {
			return nil
		}
		runID = runs[0].ID
	}
	logs, err := st.ListLogs(ctx, runID)
	if err != nil {
		return err
	}
	for _, l := range logs {
		if l.Seq > after {
			if _, err := fmt.Fprintf(stdout, "[%s] %s", l.Stream, l.Chunk); err != nil {
				return err
			}
		}
	}
	return nil
}

func triggerRun(ctx context.Context, st store.Store, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: sched run <job-id>")
	}
	job, err := st.GetJob(ctx, args[0])
	if err != nil {
		return err
	}
	now := time.Now()
	run := store.Run{ID: newID(), JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: now, CreatedAt: now}
	if err := st.CreateRun(ctx, run); err != nil {
		return err
	}
	fmt.Fprintln(stdout, run.ID)
	return nil
}

func retryRun(ctx context.Context, st store.Store, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: sched retry <run-id>")
	}
	previous, err := st.GetRun(ctx, args[0])
	if err != nil {
		return err
	}
	if previous.Status != "failed" && previous.Status != "dead" {
		return fmt.Errorf("run %s is %s; only failed or dead runs can be retried", previous.ID, previous.Status)
	}
	if _, err := st.GetJob(ctx, previous.JobID); err != nil {
		return err
	}
	now := time.Now()
	run := store.Run{ID: newID(), JobID: previous.JobID, Status: "pending", Attempt: previous.Attempt + 1, ScheduledAt: now, CreatedAt: now}
	if err := st.CreateRun(ctx, run); err != nil {
		return err
	}
	fmt.Fprintln(stdout, run.ID)
	return nil
}

func setStatus(ctx context.Context, st store.Store, args []string, status string, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: sched %s <job-id>", statusCommand(status))
	}
	if err := st.SetJobStatus(ctx, args[0], status); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s %s\n", args[0], status)
	return nil
}

func statusCommand(status string) string {
	if status == "paused" {
		return "pause"
	}
	return "resume"
}

func deleteJob(ctx context.Context, st store.Store, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: sched delete <job-id>")
	}
	if err := st.DeleteJob(ctx, args[0]); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "deleted %s\n", args[0])
	return nil
}

func pruneRuns(ctx context.Context, st store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("prune", flag.ContinueOnError)
	fs.SetOutput(stderr)
	olderThan := fs.Duration("older-than", 30*24*time.Hour, "minimum age of terminal runs to delete")
	statusCSV := fs.String("status", "succeeded,failed,dead,missed", "comma-separated terminal run statuses")
	dryRun := fs.Bool("dry-run", false, "count matching runs without deleting")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("prune does not accept positional arguments")
	}
	if *olderThan <= 0 {
		return errors.New("--older-than must be positive")
	}
	var statuses []string
	for _, status := range strings.Split(*statusCSV, ",") {
		statuses = append(statuses, strings.TrimSpace(status))
	}
	statuses, err := store.ValidatePruneStatuses(statuses)
	if err != nil {
		return err
	}
	retention, ok := st.(store.RetentionStore)
	if !ok {
		return errors.New("store does not support retention")
	}
	cutoff := time.Now().UTC().Add(-*olderThan)
	count, err := retention.PruneRuns(ctx, cutoff, statuses, *dryRun)
	if err != nil {
		return err
	}
	if *dryRun {
		fmt.Fprintf(stdout, "would prune %d runs older than %s\n", count, cutoff.Format(time.RFC3339))
	} else {
		fmt.Fprintf(stdout, "pruned %d runs older than %s\n", count, cutoff.Format(time.RFC3339))
	}
	return nil
}

func backupStore(ctx context.Context, st store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	output := fs.String("output", "", "destination file for the snapshot")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("backup does not accept positional arguments")
	}
	if strings.TrimSpace(*output) == "" {
		return errors.New("backup requires --output")
	}
	backup, ok := st.(store.BackupStore)
	if !ok {
		return errors.New("backup is only supported for the SQLite store; use Redis persistence tooling instead")
	}
	if err := backup.Backup(ctx, *output); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "backed up SQLite store to %s\n", *output)
	return nil
}

func newID() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
