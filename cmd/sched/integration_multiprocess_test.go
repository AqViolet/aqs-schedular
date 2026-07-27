package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
	"github.com/alicebob/miniredis/v2"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func buildSchedBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "sched-e2e")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = "."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build sched: %v\n%s", err, out)
	}
	return bin
}

func startSched(t *testing.T, bin string, env []string, args ...string) (*exec.Cmd, *lockedBuffer) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	logs := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd, logs
}

func stopSched(t *testing.T, cmd *exec.Cmd, logs *lockedBuffer) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("process exit: %v\n%s", err, logs.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("process did not stop\n%s", logs.String())
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestMultiProcessRedisSchedulerAndWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process test")
	}
	bin := buildSchedBinary(t)
	server := miniredis.RunT(t)
	prefix := "e2e-" + strings.ReplaceAll(t.Name(), "/", "-")
	env := []string{
		"AQS_STORE=redis",
		"AQS_REDIS_URL=redis://" + server.Addr(),
		"AQS_REDIS_PREFIX=" + prefix,
		"AQS_LEADER_TTL=3s",
		"AQS_LEADER_RETRY=100ms",
		"AQS_CONCURRENCY=2",
	}
	scheduler, schedulerLogs := startSched(t, bin, env, "scheduler")
	worker, workerLogs := startSched(t, bin, env, "worker", "--concurrency", "2")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st, err := store.OpenRedis("redis://"+server.Addr(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	added := &lockedBuffer{}
	addCmd := exec.CommandContext(ctx, bin, "add", "--schedule", "@every 1s", "--name", "multi", "--timeout", "5", "--", "echo", "multi-process-ok")
	addCmd.Env = append(os.Environ(), env...)
	addCmd.Stdout, addCmd.Stderr = added, added
	if err := addCmd.Run(); err != nil {
		t.Fatalf("add job: %v\n%s\nscheduler:\n%s\nworker:\n%s", err, added.String(), schedulerLogs.String(), workerLogs.String())
	}
	jobID := strings.TrimSpace(added.String())
	if jobID == "" {
		t.Fatal("add returned no job ID")
	}

	var run store.Run
	waitFor(t, 15*time.Second, func() bool {
		runs, err := st.ListRuns(ctx, jobID)
		if err != nil {
			return false
		}
		for _, candidate := range runs {
			if candidate.Status == "succeeded" {
				run = candidate
				return true
			}
		}
		return false
	})
	logs, err := st.ListLogs(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for _, entry := range logs {
		output.WriteString(entry.Chunk)
	}
	if !strings.Contains(output.String(), "multi-process-ok") {
		t.Fatalf("logs = %q\nscheduler:\n%s\nworker:\n%s", output.String(), schedulerLogs.String(), workerLogs.String())
	}
	stopSched(t, worker, workerLogs)
	stopSched(t, scheduler, schedulerLogs)
}
