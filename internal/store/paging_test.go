package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPagedCollections(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var st Store
			if backend == "redis" {
				st, _ = testRedis(t)
			} else {
				var err error
				st, err = Open(filepath.Join(t.TempDir(), "paging.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { st.Close() })
			}
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			for i := 0; i < 6; i++ {
				job := Job{ID: "job" + string(rune('a'+i)), Name: "name" + string(rune('a'+i)), Schedule: "@daily", Command: "echo", Status: "active", CreatedAt: now, UpdatedAt: now}
				if err := st.CreateJob(ctx, job); err != nil {
					t.Fatal(err)
				}
			}
			jobPager, ok := st.(JobPageStore)
			if !ok {
				t.Fatal("store does not support job paging")
			}
			jobs, err := jobPager.ListJobsPage(ctx, "active", 2, 1)
			if err != nil || len(jobs) != 2 || jobs[0].ID != "jobb" || jobs[1].ID != "jobc" {
				t.Fatalf("jobs=%+v err=%v", jobs, err)
			}
			runJob := Job{ID: "runs", Name: "runs", Schedule: "@daily", Command: "echo", Status: "active", CreatedAt: now, UpdatedAt: now}
			if err := st.CreateJob(ctx, runJob); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 6; i++ {
				run := Run{ID: "run" + string(rune('a'+i)), JobID: runJob.ID, Status: "succeeded", Attempt: 1, ScheduledAt: now.Add(time.Duration(i) * time.Minute), CreatedAt: now}
				if err := st.CreateRun(ctx, run); err != nil {
					t.Fatal(err)
				}
			}
			runPager := st.(RunPageStore)
			runs, err := runPager.ListRunsPage(ctx, runJob.ID, "succeeded", 2, 1)
			if err != nil || len(runs) != 2 || runs[0].ID != "rune" || runs[1].ID != "rund" {
				t.Fatalf("runs=%+v err=%v", runs, err)
			}
			for seq := 1; seq <= 12; seq++ {
				if err := st.AppendLog(ctx, Log{RunID: "runa", Stream: "stdout", Chunk: "x", Seq: int64(seq), CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			logPager := st.(LogPageStore)
			logs, err := logPager.ListLogsPage(ctx, "runa", 3, 0, 0)
			if err != nil || len(logs) != 3 || logs[0].Seq != 10 || logs[2].Seq != 12 {
				t.Fatalf("tail=%+v err=%v", logs, err)
			}
			logs, err = logPager.ListLogsPage(ctx, "runa", 3, 11, 0)
			if err != nil || len(logs) != 3 || logs[0].Seq != 8 || logs[2].Seq != 10 {
				t.Fatalf("before=%+v err=%v", logs, err)
			}
			logs, err = logPager.ListLogsPage(ctx, "runa", 2, 0, 9)
			if err != nil || len(logs) != 2 || logs[0].Seq != 10 || logs[1].Seq != 11 {
				t.Fatalf("after=%+v err=%v", logs, err)
			}
		})
	}
}
