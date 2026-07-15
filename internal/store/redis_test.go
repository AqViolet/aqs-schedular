package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func testRedis(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	s, err := OpenRedis("redis://"+server.Addr(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, server
}

func seedRedis(t *testing.T, s *Redis, now time.Time) Run {
	t.Helper()
	ctx := context.Background()
	job := Job{ID: "job", Name: "job", Status: "active", NextRunAt: &now}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run", JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: now, CreatedAt: now}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestStoreCRUDParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var s Store
			if backend == "redis" {
				s, _ = testRedis(t)
			} else {
				var err error
				s, err = Open(filepath.Join(t.TempDir(), "store.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
			}
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
			next := now.Add(time.Minute)
			job := Job{ID: "job", Name: "beta", Schedule: "* * * * *", Command: "echo hi", TimeoutSeconds: 30, MaxRetries: 2, Backoff: "linear", Status: "active", NextRunAt: &now, CreatedAt: now, UpdatedAt: now}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetJob(ctx, job.ID)
			got = normalizeJob(got)
			if err != nil || !reflect.DeepEqual(got, job) {
				t.Fatalf("job: %+v %v", got, err)
			}
			if err := s.CreateJob(ctx, job); err == nil {
				t.Fatal("duplicate job accepted")
			}
			other := job
			other.ID, other.Name, other.Status = "other", "alpha", "paused"
			if err := s.CreateJob(ctx, other); err != nil {
				t.Fatal(err)
			}
			jobs, err := s.ListJobs(ctx, "")
			if err != nil || len(jobs) != 2 || jobs[0].ID != "other" {
				t.Fatalf("jobs: %+v %v", jobs, err)
			}
			jobs, err = s.ListJobs(ctx, "active")
			if err != nil || len(jobs) != 1 {
				t.Fatalf("filtered jobs: %+v %v", jobs, err)
			}
			due, err := s.DueJobs(ctx, now)
			if err != nil || len(due) != 1 {
				t.Fatalf("due: %+v %v", due, err)
			}
			due, err = s.DueJobs(ctx, now.Add(-time.Second))
			if err != nil || len(due) != 0 {
				t.Fatalf("early due: %+v %v", due, err)
			}
			job.Name, job.Command, job.CreatedAt = "edited", "echo edited", next
			if err := s.UpdateJob(ctx, job); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.Name != "edited" || !got.CreatedAt.Equal(now) {
				t.Fatalf("updated job: %+v %v", got, err)
			}
			if err := s.SetJobStatus(ctx, job.ID, "paused"); err != nil {
				t.Fatal(err)
			}
			due, err = s.DueJobs(ctx, next)
			if err != nil || len(due) != 0 {
				t.Fatalf("paused due: %+v %v", due, err)
			}
			if err := s.SetJobStatus(ctx, job.ID, "active"); err != nil {
				t.Fatal(err)
			}
			run := Run{ID: "run", JobID: job.ID, Status: "pending", Attempt: 1, ScheduledAt: now, CreatedAt: now}
			if err := s.ScheduleJobRun(ctx, job.ID, run, &next); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetJob(ctx, job.ID)
			if err != nil || got.NextRunAt == nil || !got.NextRunAt.Equal(next) {
				t.Fatalf("advanced: %+v %v", got, err)
			}
			future := run
			future.ID, future.ScheduledAt = "future", time.Now().Add(time.Hour)
			if err := s.CreateRun(ctx, future); err != nil {
				t.Fatal(err)
			}
			if err := s.CreateRun(ctx, future); err == nil {
				t.Fatal("duplicate run accepted")
			}
			orphan := run
			orphan.ID, orphan.JobID = "orphan", "missing"
			if err := s.CreateRun(ctx, orphan); err == nil {
				t.Fatal("orphan run accepted")
			}
			pending, err := s.PendingRuns(ctx, 0)
			if err != nil || len(pending) != 1 || pending[0].ID != run.ID {
				t.Fatalf("pending: %+v %v", pending, err)
			}
			claimed, err := s.ClaimRun(ctx, future.ID, now)
			if err != nil || claimed {
				t.Fatalf("early claim: %v %v", claimed, err)
			}
			claimed, err = s.ClaimRun(ctx, run.ID, now)
			if err != nil || !claimed {
				t.Fatalf("claim: %v %v", claimed, err)
			}
			claimed, err = s.ClaimRun(ctx, run.ID, now)
			if err != nil || claimed {
				t.Fatalf("duplicate claim: %v %v", claimed, err)
			}
			running, err := s.RunningRuns(ctx)
			if err != nil || len(running) != 1 {
				t.Fatalf("running: %+v %v", running, err)
			}
			exit, duration := 0, int64(123)
			run.Status, run.StartedAt, run.FinishedAt, run.ExitCode, run.DurationMS = "succeeded", &now, &next, &exit, &duration
			if err := s.UpdateRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			gotRun, err := s.GetRun(ctx, run.ID)
			gotRun = normalizeRun(gotRun)
			if gotRun.StartedAt != nil {
				v := gotRun.StartedAt.UTC()
				gotRun.StartedAt = &v
			}
			if gotRun.FinishedAt != nil {
				v := gotRun.FinishedAt.UTC()
				gotRun.FinishedAt = &v
			}
			if err != nil || !reflect.DeepEqual(run, gotRun) {
				t.Fatalf("run: %+v %v", gotRun, err)
			}
			for _, seq := range []int64{3, 1, 2} {
				if err := s.AppendLog(ctx, Log{RunID: run.ID, Stream: "stdout", Chunk: fmt.Sprint(seq), Seq: seq, CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.AppendLog(ctx, Log{RunID: run.ID, Seq: 1}); err == nil {
				t.Fatal("duplicate log accepted")
			}
			if err := s.AppendLog(ctx, Log{RunID: "missing", Seq: 1}); err == nil {
				t.Fatal("orphan log accepted")
			}
			logs, err := s.ListLogs(ctx, run.ID)
			if err != nil || len(logs) != 3 || logs[0].Seq != 1 || logs[2].Seq != 3 {
				t.Fatalf("logs: %+v %v", logs, err)
			}
			runs, err := s.ListRuns(ctx, job.ID)
			if err != nil || len(runs) != 2 || runs[0].ID != future.ID {
				t.Fatalf("runs: %+v %v", runs, err)
			}
			if err := s.DeleteJob(ctx, job.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GetJob(ctx, job.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("deleted job: %v", err)
			}
			if _, err := s.GetRun(ctx, run.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("deleted run: %v", err)
			}
			logs, err = s.ListLogs(ctx, run.ID)
			if err != nil || len(logs) != 0 {
				t.Fatalf("cascade logs: %+v %v", logs, err)
			}
			runs, err = s.ListRuns(ctx, job.ID)
			if err != nil || len(runs) != 0 {
				t.Fatalf("cascade runs: %+v %v", runs, err)
			}
			for _, err := range []error{s.DeleteJob(ctx, "missing"), s.SetJobStatus(ctx, "missing", "active"), s.UpdateJob(ctx, Job{ID: "missing"}), s.UpdateRun(ctx, Run{ID: "missing"})} {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("missing row: %v", err)
				}
			}
		})
	}
}

func TestRedisConcurrentSchedulers(t *testing.T) {
	for _, oneShot := range []bool{false, true} {
		t.Run(fmt.Sprint(oneShot), func(t *testing.T) {
			s, server := testRedis(t)
			peer, err := OpenRedis("redis://"+server.Addr(), "")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			ctx := context.Background()
			now := time.Now().UTC()
			if err := s.CreateJob(ctx, Job{ID: "job", Status: "active", NextRunAt: &now}); err != nil {
				t.Fatal(err)
			}
			var next *time.Time
			if !oneShot {
				v := now.Add(time.Minute)
				next = &v
			}
			var wg sync.WaitGroup
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					st := s
					if i%2 == 0 {
						st = peer
					}
					r := Run{ID: fmt.Sprint(i), JobID: "job", Status: "pending", ScheduledAt: now, Attempt: 1}
					if err := st.ScheduleJobRun(ctx, "job", r, next); err != nil {
						t.Errorf("schedule: %v", err)
					}
				}(i)
			}
			wg.Wait()
			runs, err := s.ListRuns(ctx, "job")
			if err != nil || len(runs) != 1 {
				t.Fatalf("scheduled: %+v %v", runs, err)
			}
			job, err := s.GetJob(ctx, "job")
			if err != nil || (oneShot && job.NextRunAt != nil) || (!oneShot && (job.NextRunAt == nil || !job.NextRunAt.Equal(*next))) {
				t.Fatalf("job: %+v %v", job, err)
			}
		})
	}
}

func TestRedisSchedulerValidationAndRollback(t *testing.T) {
	s, _ := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	next := now.Add(time.Minute)
	if err := s.ScheduleJobRun(ctx, "job", r, &next); err == nil {
		t.Fatal("duplicate run accepted")
	}
	job, err := s.GetJob(ctx, "job")
	if err != nil || !job.NextRunAt.Equal(now) {
		t.Fatalf("failed scheduling advanced job: %+v %v", job, err)
	}
	r.ID = "new"
	if err := s.ScheduleJobRun(ctx, "other", r, &next); err == nil {
		t.Fatal("mismatch accepted")
	}
	if err := s.ScheduleJobRun(ctx, "job", r, &now); err == nil {
		t.Fatal("non-advancing schedule accepted")
	}
	if err := s.SetJobStatus(ctx, "job", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleJobRun(ctx, "job", r, &next); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, r.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("paused scheduled: %v", err)
	}
	if err := s.SetJobStatus(ctx, "job", "active"); err != nil {
		t.Fatal(err)
	}
	r.ScheduledAt = now.Add(-time.Second)
	if err := s.ScheduleJobRun(ctx, "job", r, &next); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, r.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale scheduled: %v", err)
	}
}

func TestRedisConcurrentClaims(t *testing.T) {
	for _, leased := range []bool{false, true} {
		t.Run(fmt.Sprint(leased), func(t *testing.T) {
			s, server := testRedis(t)
			peer, err := OpenRedis("redis://"+server.Addr(), "")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			now := time.Now().UTC()
			r := seedRedis(t, s, now)
			var winners atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					st := s
					if i%2 == 0 {
						st = peer
					}
					var claimed bool
					var err error
					if leased {
						claimed, err = st.ClaimRunLease(context.Background(), r.ID, fmt.Sprint(i), now, time.Minute)
					} else {
						claimed, err = st.ClaimRun(context.Background(), r.ID, now)
					}
					if err != nil {
						t.Errorf("claim: %v", err)
					}
					if claimed {
						winners.Add(1)
					}
				}(i)
			}
			wg.Wait()
			if winners.Load() != 1 {
				t.Fatalf("winners: %d", winners.Load())
			}
		})
	}
}

func TestRedisLeaseRecoveryAndFencing(t *testing.T) {
	s, _ := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "old", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.AppendRunLog(ctx, "old", Log{RunID: r.ID, Seq: 1, Chunk: "before crash"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendRunLog(ctx, "wrong", Log{RunID: r.ID, Seq: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong log owner: %v", err)
	}
	if err := s.AppendLog(ctx, Log{RunID: r.ID, Seq: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unfenced log: %v", err)
	}
	if err := s.UpdateRun(ctx, r); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unfenced update: %v", err)
	}
	if ok, err := s.RenewRunLease(ctx, r.ID, "wrong", now, time.Minute); err != nil || ok {
		t.Fatalf("wrong renew: %v %v", ok, err)
	}
	if ok, err := s.RenewRunLease(ctx, r.ID, "old", now.Add(30*time.Second), time.Minute); err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	if err := s.RecoverExpiredRuns(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, r.ID)
	if err != nil || got.Status != "running" {
		t.Fatalf("renewed run recovered: %+v %v", got, err)
	}
	recoveredAt := now.Add(90 * time.Second)
	if ok, err := s.RenewRunLease(ctx, r.ID, "old", recoveredAt, time.Minute); err != nil || ok {
		t.Fatalf("expired renew: %v %v", ok, err)
	}
	if err := s.RecoverExpiredRuns(ctx, recoveredAt); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetRun(ctx, r.ID)
	if err != nil || got.ID != r.ID || got.Status != "pending" || got.Attempt != 2 || got.StartedAt != nil || got.FinishedAt != nil || got.ExitCode != nil || got.DurationMS != nil || got.Error == "" || !got.ScheduledAt.Equal(recoveredAt) {
		t.Fatalf("recovered: %+v %v", got, err)
	}
	if err := s.RecoverExpiredRuns(ctx, recoveredAt); err != nil {
		t.Fatal(err)
	}
	again, _ := s.GetRun(ctx, r.ID)
	if again.Attempt != 2 {
		t.Fatalf("recovery repeated: %+v", again)
	}
	logs, err := s.ListLogs(ctx, r.ID)
	if err != nil || len(logs) != 1 || logs[0].Chunk != "before crash" {
		t.Fatalf("recovered logs: %+v %v", logs, err)
	}
	if ok, err := s.ClaimRunLease(ctx, r.ID, "new", recoveredAt, time.Minute); err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	r.Status = "failed"
	retry := Run{ID: "retry", JobID: r.JobID, Status: "pending", Attempt: 2, ScheduledAt: recoveredAt}
	if err := s.CompleteRun(ctx, r, "old", &retry); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if _, err := s.GetRun(ctx, retry.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale retry exists: %v", err)
	}
	if err := s.AppendRunLog(ctx, "old", Log{RunID: r.ID, Seq: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale log: %v", err)
	}
	if err := s.AppendRunLog(ctx, "new", Log{RunID: r.ID, Seq: 2}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetRun(ctx, r.ID)
	got.Status, got.FinishedAt = "succeeded", &recoveredAt
	if err := s.CompleteRun(ctx, got, "new", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteRun(ctx, got, "new", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("repeat completion: %v", err)
	}
	if err := s.AppendRunLog(ctx, "new", Log{RunID: r.ID, Seq: 3}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("finished log: %v", err)
	}
}

func TestRedisAtomicRetry(t *testing.T) {
	s, _ := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	r, _ = s.GetRun(ctx, r.ID)
	exit, duration := 1, int64(50)
	r.Status, r.FinishedAt, r.ExitCode, r.DurationMS, r.Error = "failed", &now, &exit, &duration, "command failed"
	retry := Run{ID: "retry", JobID: r.JobID, Status: "pending", Attempt: 2, ScheduledAt: now.Add(time.Second)}
	invalid := retry
	invalid.JobID = "other"
	if err := s.CompleteRun(ctx, r, "owner", &invalid); err == nil {
		t.Fatal("invalid retry accepted")
	}
	if err := s.CreateRun(ctx, retry); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteRun(ctx, r, "owner", &retry); err == nil {
		t.Fatal("duplicate retry accepted")
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.Status != "running" {
		t.Fatalf("failed retry completed run: %+v", got)
	}
	if err := s.AppendRunLog(ctx, "owner", Log{RunID: r.ID, Seq: 1}); err != nil {
		t.Fatalf("failed retry lost lease: %v", err)
	}
	retry.ID = "retry-new"
	if err := s.CompleteRun(ctx, r, "owner", &retry); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, r.ID)
	if err != nil || got.Status != "failed" || got.ExitCode == nil || *got.ExitCode != 1 || got.DurationMS == nil || *got.DurationMS != 50 {
		t.Fatalf("completed: %+v %v", got, err)
	}
	got, err = s.GetRun(ctx, retry.ID)
	if err != nil || got.Status != "pending" || got.Attempt != 2 || !got.ScheduledAt.Equal(retry.ScheduledAt) {
		t.Fatalf("retry: %+v %v", got, err)
	}
}

func TestRedisDeletionFencesWorkers(t *testing.T) {
	s, _ := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.AppendRunLog(ctx, "owner", Log{RunID: r.ID, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteJob(ctx, r.JobID); err != nil {
		t.Fatal(err)
	}
	r.Status = "failed"
	if err := s.CompleteRun(ctx, r, "owner", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("deleted completion: %v", err)
	}
	if err := s.AppendRunLog(ctx, "owner", Log{RunID: r.ID, Seq: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("deleted log: %v", err)
	}
	if ok, err := s.RenewRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || ok {
		t.Fatalf("deleted renew: %v %v", ok, err)
	}
	if err := s.RecoverExpiredRuns(ctx, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.client.HLen(ctx, s.keys[3]).Result(); err != nil || n != 0 {
		t.Fatalf("orphan leases: %d %v", n, err)
	}
	logs, err := s.ListLogs(ctx, r.ID)
	if err != nil || len(logs) != 0 {
		t.Fatalf("deleted logs: %+v %v", logs, err)
	}
	seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "different", now, time.Minute); err != nil || !ok {
		t.Fatalf("recreated claim: %v %v", ok, err)
	}
	if err := s.CompleteRun(ctx, r, "owner", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("recreated completion: %v", err)
	}
}

func TestRedisReopenAndNamespace(t *testing.T) {
	s, server := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.AppendRunLog(ctx, "owner", Log{RunID: r.ID, Seq: 1, Chunk: "persisted"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRedis("redis://"+server.Addr(), "aqs-scheduler")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetRun(ctx, r.ID)
	if err != nil || got.Status != "running" {
		t.Fatalf("reopened run: %+v %v", got, err)
	}
	if _, err := reopened.GetJob(ctx, r.JobID); err != nil {
		t.Fatal(err)
	}
	logs, err := reopened.ListLogs(ctx, r.ID)
	if err != nil || len(logs) != 1 || logs[0].Chunk != "persisted" {
		t.Fatalf("reopened logs: %+v %v", logs, err)
	}
	if ok, err := reopened.RenewRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("reopened lease: %v %v", ok, err)
	}
	isolated, err := OpenRedis("redis://"+server.Addr(), "isolated")
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	if _, err := isolated.GetJob(ctx, r.JobID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("namespace leaked: %v", err)
	}
	seedRedis(t, isolated, now)
}

func TestRedisServerExpiryAndValidation(t *testing.T) {
	s, server := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	server.SetTime(now)
	r := seedRedis(t, s, now)
	if _, err := OpenRedis("not-a-url", ""); err == nil {
		t.Fatal("invalid URL accepted")
	}
	for _, test := range []struct {
		owner string
		ttl   time.Duration
	}{{"", time.Minute}, {"owner", 0}, {"owner", -time.Second}, {"owner", time.Nanosecond}} {
		if _, err := s.ClaimRunLease(ctx, r.ID, test.owner, now, test.ttl); err == nil {
			t.Fatal("invalid lease accepted")
		}
		if _, err := s.RenewRunLease(ctx, r.ID, test.owner, now, test.ttl); err == nil {
			t.Fatal("invalid renewal accepted")
		}
	}
	if ok, err := s.ClaimRunLease(ctx, "missing", "owner", now, time.Minute); err != nil || ok {
		t.Fatalf("missing claim: %v %v", ok, err)
	}
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now.Add(-time.Nanosecond), time.Minute); err != nil || ok {
		t.Fatalf("early claim: %v %v", ok, err)
	}
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	server.SetTime(now.Add(time.Minute))
	r.Status = "succeeded"
	if err := s.CompleteRun(ctx, r, "owner", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired completion: %v", err)
	}
	if err := s.AppendRunLog(ctx, "owner", Log{RunID: r.ID, Seq: 1}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired log: %v", err)
	}
	if ok, err := s.RenewRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || ok {
		t.Fatalf("server-expired renewal: %v %v", ok, err)
	}
	if err := s.RecoverExpiredRuns(ctx, now); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, r.ID)
	if err != nil || got.Status != "pending" || got.Attempt != 2 {
		t.Fatalf("server-expired recovery: %+v %v", got, err)
	}
}

func TestRedisNativeTTLRecoveryClearsResults(t *testing.T) {
	s, server := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	exit, duration := 42, int64(1000)
	r.StartedAt, r.FinishedAt, r.ExitCode, r.DurationMS, r.Error = &now, &now, &exit, &duration, "old error"
	if err := s.UpdateRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ClaimRunLease(ctx, r.ID, "old", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.AppendRunLog(ctx, "old", Log{RunID: r.ID, Seq: 9007199254740993, Chunk: "retained"}); err != nil {
		t.Fatal(err)
	}
	server.FastForward(time.Minute)
	r.Status = "dead"
	if err := s.CompleteRun(ctx, r, "old", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("TTL-expired completion: %v", err)
	}
	if err := s.AppendRunLog(ctx, "old", Log{RunID: r.ID, Seq: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("TTL-expired log: %v", err)
	}
	if err := s.RecoverExpiredRuns(ctx, now); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, r.ID)
	if err != nil || got.Status != "pending" || got.Attempt != 2 || got.StartedAt != nil || got.FinishedAt != nil || got.ExitCode != nil || got.DurationMS != nil || got.Error != "interrupted: worker lease expired" {
		t.Fatalf("recovery: %+v %v", got, err)
	}
	logs, err := s.ListLogs(ctx, r.ID)
	if err != nil || len(logs) != 1 || logs[0].Seq != 9007199254740993 || logs[0].Chunk != "retained" {
		t.Fatalf("logs: %+v %v", logs, err)
	}
	if ok, err := s.ClaimRunLease(ctx, r.ID, "new", now, time.Minute); err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	got, _ = s.GetRun(ctx, r.ID)
	got.Status = "dead"
	if err := s.CompleteRun(ctx, got, "new", nil); err != nil {
		t.Fatal(err)
	}
}

func TestRedisConcurrentCompletionAndRetry(t *testing.T) {
	s, _ := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	r, _ = s.GetRun(ctx, r.ID)
	r.Status = "failed"
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			retry := Run{ID: fmt.Sprintf("retry-%d", i), JobID: r.JobID, Status: "pending", Attempt: 2, ScheduledAt: now}
			err := s.CompleteRun(ctx, r, "owner", &retry)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, ErrLeaseLost) {
				t.Errorf("complete: %v", err)
			}
		}(i)
	}
	wg.Wait()
	runs, err := s.ListRuns(ctx, r.JobID)
	if err != nil || len(runs) != 2 || winners.Load() != 1 {
		t.Fatalf("atomic completion: runs=%+v winners=%d err=%v", runs, winners.Load(), err)
	}
}

func TestRedisConcurrentRecovery(t *testing.T) {
	s, server := testRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()
	r := seedRedis(t, s, now)
	if ok, err := s.ClaimRunLease(ctx, r.ID, "owner", now, time.Minute); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	server.FastForward(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.RecoverExpiredRuns(ctx, now); err != nil {
				t.Errorf("recover: %v", err)
			}
		}()
	}
	wg.Wait()
	got, err := s.GetRun(ctx, r.ID)
	if err != nil || got.Status != "pending" || got.Attempt != 2 {
		t.Fatalf("recovery: %+v %v", got, err)
	}
}
