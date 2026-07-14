package coordination

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestFromEnvUsesLocalModeWithoutRedisURL(t *testing.T) {
	t.Setenv("AQS_REDIS_URL", "")
	leader, err := FromEnv(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := leader.(Local); !ok {
		t.Fatalf("leader = %T, want Local", leader)
	}
}

func TestFromEnvConfiguresRedisLeader(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("AQS_REDIS_URL", "redis://"+server.Addr()+"/0")
	t.Setenv("AQS_LEADER_KEY", "service:active-leader")
	t.Setenv("AQS_LEADER_TTL", "9s")
	t.Setenv("AQS_LEADER_RETRY", "250ms")
	leader, err := FromEnv(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	redisLeader, ok := leader.(*RedisLeader)
	if !ok {
		t.Fatalf("leader = %T, want *RedisLeader", leader)
	}
	if redisLeader.key != "service:active-leader" || redisLeader.ttl != 9*time.Second || redisLeader.retry != 250*time.Millisecond {
		t.Fatalf("configuration: key=%q ttl=%s retry=%s", redisLeader.key, redisLeader.ttl, redisLeader.retry)
	}
}

func TestFromEnvRejectsInvalidTTL(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("AQS_REDIS_URL", "redis://"+server.Addr()+"/0")
	t.Setenv("AQS_LEADER_TTL", "1s")
	if _, err := FromEnv(slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("expected invalid TTL error")
	}
}
