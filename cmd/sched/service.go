package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/alerts"
	"github.com/AqViolet/aqs-schedular/internal/api"
	"github.com/AqViolet/aqs-schedular/internal/config"
	"github.com/AqViolet/aqs-schedular/internal/coordination"
	"github.com/AqViolet/aqs-schedular/internal/executor"
	"github.com/AqViolet/aqs-schedular/internal/scheduler"
	"github.com/AqViolet/aqs-schedular/internal/store"
)

func runService(role string, args []string, settings config.Settings, stderr io.Writer) error {
	fs := flag.NewFlagSet(role, flag.ContinueOnError)
	fs.SetOutput(stderr)
	concurrency := fs.Int("concurrency", settings.Concurrency, "worker concurrency (1-256)")
	defaultListen := settings.APIListen
	if role == "serve" && defaultListen == "" {
		defaultListen = "127.0.0.1:8080"
	}
	listen := fs.String("listen", defaultListen, "management API address (requires AQS_API_TOKEN)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s does not accept positional arguments", role)
	}
	if *concurrency < 1 || *concurrency > 256 {
		return errors.New("concurrency must be between 1 and 256")
	}
	if (role == "worker" || role == "scheduler") && settings.Backend != "redis" {
		return errors.New("separate scheduler and worker roles require AQS_STORE=redis")
	}
	if *listen != "" && strings.TrimSpace(settings.APIToken) == "" {
		return errors.New("AQS_API_TOKEN is required when the management API is enabled")
	}
	settings.Concurrency = *concurrency
	logger := slog.New(slog.NewJSONHandler(stderr, nil)).With("role", role, "pid", os.Getpid())
	st, err := settings.OpenStore()
	if err != nil {
		return err
	}
	defer st.Close()
	var webhook *alerts.Webhook
	if settings.AlertURL != "" {
		webhook, err = alerts.New(settings.AlertURL, settings.AlertSecret, settings.AlertStatuses, logger)
		if err != nil {
			return err
		}
	}
	var outbox *alerts.Outbox
	if webhook != nil {
		if alertStore, ok := st.(store.AlertStore); ok {
			cfg := alerts.DefaultOutboxConfig()
			cfg.MaxAttempts, cfg.RetryBase = settings.AlertMax, settings.AlertRetry
			cfg.Lease, cfg.SweepLookback = settings.AlertLease, settings.AlertLookback
			outbox, err = alerts.NewOutbox(alertStore, webhook, cfg, logger)
			if err != nil {
				return err
			}
		}
	}
	var resultHandler func(store.Job, store.Run)
	switch {
	case outbox != nil:
		resultHandler = outbox.Handler()
	case webhook != nil:
		resultHandler = webhook.Handler()
	}
	var leader coordination.Leader
	if role == "daemon" || role == "scheduler" {
		leader, err = coordination.FromEnv(logger)
		if err != nil {
			return err
		}
		defer leader.Close()
	}
	var server *http.Server
	var listener net.Listener
	if *listen != "" {
		listener, err = net.Listen("tcp", *listen)
		if err != nil {
			return fmt.Errorf("listen for API: %w", err)
		}
		defer listener.Close()
		server = &http.Server{
			Handler:           api.New(st, settings.APIToken, logger),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
			WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			MaxHeaderBytes: 16 * 1024,
		}
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	failures := make(chan error, 3)
	var tasks sync.WaitGroup
	startTask := func(run func() error) {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			if err := run(); err != nil && ctx.Err() == nil {
				failures <- err
			}
		}()
	}
	var pool *executor.Pool
	if settings.Backend == "redis" && (role == "daemon" || role == "worker") {
		pool, err = newWorkerPool(st, settings, logger, resultHandler)
		if err != nil {
			return err
		}
		pool.Start()
	}
	if outbox != nil && (role == "daemon" || role == "worker") {
		startTask(func() error { return outbox.Run(ctx) })
	}
	if role == "daemon" && settings.Retention > 0 {
		startTask(func() error { return retentionLoop(ctx, st, settings.Retention, logger) })
	}
	if role == "daemon" || role == "scheduler" {
		startTask(func() error {
			return leader.Run(ctx, func(leaderCtx context.Context) error {
				logger.Info("scheduler leadership active")
				if settings.Backend == "sqlite" {
					return runLocalNode(leaderCtx, st, settings, logger, resultHandler)
				}
				return scheduler.New(st, logger).Run(leaderCtx)
			})
		})
	}
	if server != nil {
		logger.Info("management API listening", "address", listener.Addr().String())
		startTask(func() error {
			err := server.Serve(listener)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	logger.Info("service started", "store", settings.Backend, "workers", settings.Concurrency)
	var serviceErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case serviceErr = <-failures:
		logger.Error("service failed", "error", serviceErr)
	}
	cancel()
	if pool != nil {
		pool.Stop()
	}
	if server != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Shutdown(shutdownCtx); err != nil {
			server.Close()
			logger.Warn("API shutdown interrupted", "error", err)
		}
		shutdownCancel()
	}
	if pool != nil {
		if err := shutdownPool(pool, st, logger, false, false); err != nil && serviceErr == nil {
			serviceErr = err
		}
	}
	done := make(chan struct{})
	go func() { tasks.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(35 * time.Second):
		return errors.New("service shutdown timed out")
	}
	logger.Info("service stopped")
	return serviceErr
}

func retentionLoop(ctx context.Context, st store.Store, olderThan time.Duration, logger *slog.Logger) error {
	retention, ok := st.(store.RetentionStore)
	if !ok {
		return nil
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		count, err := retention.PruneRuns(ctx, time.Now().UTC().Add(-olderThan), nil, false)
		if err != nil {
			logger.Warn("retention prune failed")
		} else if count > 0 {
			logger.Info("pruned old runs", "count", count, "older_than", olderThan)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func newWorkerPool(st store.Store, settings config.Settings, logger *slog.Logger, resultHandler func(store.Job, store.Run)) (*executor.Pool, error) {
	pool := executor.New(st, settings.Concurrency, logger)
	if err := pool.SetLeaseTTL(settings.WorkerLease); err != nil {
		return nil, err
	}
	if resultHandler != nil {
		pool.SetResultHandler(resultHandler)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Recover(ctx); err != nil {
		return nil, err
	}
	return pool, nil
}

func runLocalNode(ctx context.Context, st store.Store, settings config.Settings, logger *slog.Logger, resultHandler func(store.Job, store.Run)) error {
	pool, err := newWorkerPool(st, settings, logger, resultHandler)
	if err != nil {
		return err
	}
	pool.Start()
	nodeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- scheduler.New(st, logger).Run(nodeCtx) }()
	var loopErr error
	select {
	case <-ctx.Done():
		cancel()
		loopErr = <-done
	case loopErr = <-done:
		cancel()
	}
	shutdownErr := shutdownPool(pool, st, logger, true, errors.Is(context.Cause(ctx), coordination.ErrLeadershipLost))
	if loopErr != nil {
		return loopErr
	}
	return shutdownErr
}

func shutdownPool(pool *executor.Pool, st store.Store, logger *slog.Logger, local, abort bool) error {
	pool.Stop()
	var inFlight []store.Run
	if local {
		var err error
		inFlight, err = st.RunningRuns(context.Background())
		if err != nil {
			logger.Warn("list in-flight runs", "error", err)
		}
		for _, run := range inFlight {
			logger.Info("waiting for in-flight run", "run_id", run.ID, "job_id", run.JobID)
		}
	}
	if abort {
		pool.Abort()
		logger.Warn("worker pool aborted after leadership loss")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pool.Shutdown(ctx)
	for _, run := range inFlight {
		current, getErr := st.GetRun(context.Background(), run.ID)
		if getErr != nil {
			logger.Warn("read shutdown result", "run_id", run.ID, "error", getErr)
		} else {
			logger.Info("in-flight run finished", "run_id", run.ID, "status", current.Status, "abandoned", err != nil)
		}
	}
	logger.Info("worker pool stopped", "abandoned", err != nil)
	return err
}
