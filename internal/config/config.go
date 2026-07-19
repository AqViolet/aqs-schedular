package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AqViolet/aqs-schedular/internal/store"
)

const DefaultConcurrency = 4

type Settings struct {
	Backend       string
	RedisURL      string
	RedisPrefix   string
	Concurrency   int
	WorkerLease   time.Duration
	APIListen     string
	APIToken      string
	AlertURL      string
	AlertSecret   string
	AlertStatuses []string
}

func Load() (Settings, error) {
	s := Settings{
		Backend:  strings.ToLower(strings.TrimSpace(os.Getenv("AQS_STORE"))),
		RedisURL: os.Getenv("AQS_REDIS_URL"), RedisPrefix: os.Getenv("AQS_REDIS_PREFIX"),
		Concurrency: DefaultConcurrency, WorkerLease: 15 * time.Second,
		APIListen: os.Getenv("AQS_API_LISTEN"), APIToken: os.Getenv("AQS_API_TOKEN"),
		AlertURL: os.Getenv("AQS_ALERT_WEBHOOK_URL"), AlertSecret: os.Getenv("AQS_ALERT_SECRET"),
	}
	if s.Backend == "" {
		s.Backend = "sqlite"
	}
	if s.Backend != "sqlite" && s.Backend != "redis" {
		return s, fmt.Errorf("AQS_STORE must be sqlite or redis")
	}
	if s.Backend == "redis" && strings.TrimSpace(s.RedisURL) == "" {
		return s, fmt.Errorf("AQS_REDIS_URL is required for the Redis store")
	}
	if s.RedisPrefix == "" {
		s.RedisPrefix = "aqs-scheduler"
	}
	if raw := os.Getenv("AQS_CONCURRENCY"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 256 {
			return s, fmt.Errorf("AQS_CONCURRENCY must be between 1 and 256")
		}
		s.Concurrency = value
	}
	if raw := os.Getenv("AQS_WORKER_LEASE_TTL"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value < 3*time.Second {
			return s, fmt.Errorf("AQS_WORKER_LEASE_TTL must be at least 3s")
		}
		s.WorkerLease = value
	}
	if raw := os.Getenv("AQS_ALERT_STATUSES"); raw != "" {
		for _, status := range strings.Split(raw, ",") {
			status = strings.TrimSpace(status)
			if status != "dead" && status != "failed" && status != "succeeded" {
				return s, fmt.Errorf("AQS_ALERT_STATUSES accepts dead, failed, succeeded")
			}
			s.AlertStatuses = append(s.AlertStatuses, status)
		}
	}
	return s, nil
}

func (s Settings) OpenStore() (store.Store, error) {
	if s.Backend == "redis" {
		return store.OpenRedis(s.RedisURL, s.RedisPrefix)
	}
	return store.Open(DatabasePath())
}

func DatabasePath() string {
	if path := os.Getenv("AQS_DB"); path != "" {
		return path
	}
	return "aqs.db"
}
