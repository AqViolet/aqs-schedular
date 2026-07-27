package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteBackupSnapshot(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	job := Job{ID: "job", Name: "backup", Schedule: "@daily", Command: "echo ok", Status: "active", CreatedAt: now, UpdatedAt: now}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run", JobID: job.ID, Status: "succeeded", Attempt: 1, ScheduledAt: now, CreatedAt: now}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendLog(ctx, Log{RunID: run.ID, Stream: "stdout", Chunk: "hello", Seq: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	backup, ok := any(st).(BackupStore)
	if !ok {
		t.Fatal("SQLite store does not support backup")
	}
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := backup.Backup(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := backup.Backup(ctx, path); err == nil {
		t.Fatal("overwrote an existing backup")
	}
	if err := backup.Backup(ctx, ""); err == nil {
		t.Fatal("accepted an empty backup path")
	}
	snapshot, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	gotJob, err := snapshot.GetJob(ctx, job.ID)
	if err != nil || gotJob.Name != job.Name {
		t.Fatalf("backup job = %+v err=%v", gotJob, err)
	}
	logs, err := snapshot.ListLogs(ctx, run.ID)
	if err != nil || len(logs) != 1 || logs[0].Chunk != "hello" {
		t.Fatalf("backup logs = %+v err=%v", logs, err)
	}
}
