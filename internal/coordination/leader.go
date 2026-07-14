package coordination

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrLeadershipLost = errors.New("leadership lease lost")

type Leader interface {
	Run(context.Context, func(context.Context) error) error
	Close() error
}

type Local struct{}

func (Local) Run(ctx context.Context, run func(context.Context) error) error { return run(ctx) }
func (Local) Close() error                                                   { return nil }

type RedisLeader struct {
	client redis.UniversalClient
	key    string
	token  string
	ttl    time.Duration
	retry  time.Duration
	logger *slog.Logger
}

var renewScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
var releaseScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)

func NewRedis(client redis.UniversalClient, key string, ttl, retry time.Duration, logger *slog.Logger) (*RedisLeader, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("leader key is required")
	}
	if ttl < 3*time.Second {
		return nil, errors.New("leader TTL must be at least 3s")
	}
	if retry <= 0 {
		retry = time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	var tokenBytes [32]byte
	if _, err := cryptorand.Read(tokenBytes[:]); err != nil {
		return nil, fmt.Errorf("generate leader token: %w", err)
	}
	return &RedisLeader{client: client, key: key, token: hex.EncodeToString(tokenBytes[:]), ttl: ttl, retry: retry, logger: logger}, nil
}

func FromEnv(logger *slog.Logger) (Leader, error) {
	url := strings.TrimSpace(os.Getenv("AQS_REDIS_URL"))
	if url == "" {
		return Local{}, nil
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse AQS_REDIS_URL: %w", err)
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}
	key := os.Getenv("AQS_LEADER_KEY")
	if key == "" {
		key = "aqs-schedular:leader"
	}
	ttl := 15 * time.Second
	if raw := os.Getenv("AQS_LEADER_TTL"); raw != "" {
		ttl, err = time.ParseDuration(raw)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("parse AQS_LEADER_TTL: %w", err)
		}
	}
	retry := 2 * time.Second
	if raw := os.Getenv("AQS_LEADER_RETRY"); raw != "" {
		retry, err = time.ParseDuration(raw)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("parse AQS_LEADER_RETRY: %w", err)
		}
	}
	leader, err := NewRedis(client, key, ttl, retry, logger)
	if err != nil {
		client.Close()
		return nil, err
	}
	return leader, nil
}

func (r *RedisLeader) Close() error { return r.client.Close() }

func (r *RedisLeader) Run(ctx context.Context, run func(context.Context) error) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		acquired, err := r.client.SetNX(ctx, r.key, r.token, r.ttl).Result()
		if err != nil {
			r.logger.Error("acquire leader lease", "error", err)
			if !wait(ctx, r.retry) {
				return nil
			}
			continue
		}
		if !acquired {
			if !wait(ctx, r.retry) {
				return nil
			}
			continue
		}

		r.logger.Info("leader lease acquired", "key", r.key, "ttl", r.ttl)
		lost, err := r.runAsLeader(ctx, run)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		if !lost {
			return nil
		}
		r.logger.Warn("leader lease lost; retrying election", "key", r.key)
		if !wait(ctx, r.retry) {
			return nil
		}
	}
}

func (r *RedisLeader) runAsLeader(parent context.Context, run func(context.Context) error) (bool, error) {
	ctx, cancel := context.WithCancelCause(parent)
	lost := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(r.ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := r.renew(ctx)
				if err != nil || !ok {
					if err != nil {
						r.logger.Error("renew leader lease", "error", err)
					}
					cancel(ErrLeadershipLost)
					close(lost)
					return
				}
			}
		}
	}()

	runErr := run(ctx)
	cancel(context.Canceled)
	<-renewDone
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, releaseErr := r.release(releaseCtx)
	releaseCancel()
	if runErr != nil {
		return false, runErr
	}
	if releaseErr != nil {
		r.logger.Warn("release leader lease", "error", releaseErr)
	}
	select {
	case <-lost:
		return true, nil
	default:
		return false, nil
	}
}

func (r *RedisLeader) renew(ctx context.Context) (bool, error) {
	result, err := renewScript.Run(ctx, r.client, []string{r.key}, r.token, r.ttl.Milliseconds()).Int64()
	return result == 1, err
}

func (r *RedisLeader) release(ctx context.Context) (bool, error) {
	result, err := releaseScript.Run(ctx, r.client, []string{r.key}, r.token).Int64()
	return result == 1, err
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
