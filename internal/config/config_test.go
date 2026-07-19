package config

import (
	"testing"
	"time"
)

func cleanEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AQS_STORE", "AQS_REDIS_URL", "AQS_REDIS_PREFIX", "AQS_CONCURRENCY", "AQS_WORKER_LEASE_TTL", "AQS_API_LISTEN", "AQS_API_TOKEN", "AQS_ALERT_WEBHOOK_URL", "AQS_ALERT_SECRET", "AQS_ALERT_STATUSES"} {
		t.Setenv(name, "")
	}
}

func TestDefaults(t *testing.T) {
	cleanEnvironment(t)
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if s.Backend != "sqlite" || s.Concurrency != 4 || s.WorkerLease != 15*time.Second || s.APIListen != "" {
		t.Fatalf("defaults = %#v", s)
	}
}

func TestDistributedConfiguration(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("AQS_STORE", "redis")
	t.Setenv("AQS_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("AQS_REDIS_PREFIX", "test-cluster")
	t.Setenv("AQS_CONCURRENCY", "8")
	t.Setenv("AQS_WORKER_LEASE_TTL", "30s")
	t.Setenv("AQS_ALERT_STATUSES", "dead,failed")
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if s.RedisPrefix != "test-cluster" || s.Concurrency != 8 || s.WorkerLease != 30*time.Second || len(s.AlertStatuses) != 2 {
		t.Fatalf("settings = %#v", s)
	}
}

func TestInvalidSettings(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{"AQS_STORE", "postgres"}, {"AQS_STORE", "redis"},
		{"AQS_CONCURRENCY", "0"}, {"AQS_CONCURRENCY", "257"},
		{"AQS_WORKER_LEASE_TTL", "1s"}, {"AQS_ALERT_STATUSES", "running"},
	} {
		t.Run(test.name+test.value, func(t *testing.T) {
			cleanEnvironment(t)
			t.Setenv(test.name, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
