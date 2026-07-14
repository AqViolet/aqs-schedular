package coordination

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLeader(t *testing.T, server *miniredis.Miniredis, retry time.Duration) *RedisLeader {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	leader, err := NewRedis(client, "test:leader", 3*time.Second, retry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return leader
}

func TestLeaseUsesOwnerTokenForRenewAndRelease(t *testing.T) {
	server := miniredis.RunT(t)
	first := newTestLeader(t, server, time.Millisecond)
	second := newTestLeader(t, server, time.Millisecond)
	ctx := context.Background()

	acquired, err := first.client.SetNX(ctx, first.key, first.token, first.ttl).Result()
	if err != nil || !acquired {
		t.Fatalf("first acquire=%t err=%v", acquired, err)
	}
	acquired, err = second.client.SetNX(ctx, second.key, second.token, second.ttl).Result()
	if err != nil || acquired {
		t.Fatalf("second acquire=%t err=%v, want false", acquired, err)
	}
	if ok, err := first.renew(ctx); err != nil || !ok {
		t.Fatalf("owner renew=%t err=%v", ok, err)
	}
	if ok, err := second.renew(ctx); err != nil || ok {
		t.Fatalf("non-owner renew=%t err=%v, want false", ok, err)
	}
	if ok, err := first.release(ctx); err != nil || !ok {
		t.Fatalf("owner release=%t err=%v", ok, err)
	}
	acquired, err = second.client.SetNX(ctx, second.key, second.token, second.ttl).Result()
	if err != nil || !acquired {
		t.Fatalf("second acquire after release=%t err=%v", acquired, err)
	}
	if ok, err := first.release(ctx); err != nil || ok {
		t.Fatalf("stale owner release=%t err=%v, want false", ok, err)
	}
	value, err := second.client.Get(ctx, second.key).Result()
	if err != nil || value != second.token {
		t.Fatalf("lease owner changed: value=%q err=%v", value, err)
	}
}

func TestLeaseExpiresAndCanBeAcquiredByAnotherInstance(t *testing.T) {
	server := miniredis.RunT(t)
	first := newTestLeader(t, server, time.Millisecond)
	second := newTestLeader(t, server, time.Millisecond)
	ctx := context.Background()
	if ok, err := first.client.SetNX(ctx, first.key, first.token, first.ttl).Result(); err != nil || !ok {
		t.Fatalf("first acquire=%t err=%v", ok, err)
	}
	server.FastForward(first.ttl + time.Second)
	if ok, err := second.client.SetNX(ctx, second.key, second.token, second.ttl).Result(); err != nil || !ok {
		t.Fatalf("second acquire after expiry=%t err=%v", ok, err)
	}
	if ok, err := first.release(ctx); err != nil || ok {
		t.Fatalf("expired owner release=%t err=%v", ok, err)
	}
}

func TestRunElectsOneLeaderAndFailsOver(t *testing.T) {
	server := miniredis.RunT(t)
	first := newTestLeader(t, server, 10*time.Millisecond)
	second := newTestLeader(t, server, 10*time.Millisecond)
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel1()
	defer cancel2()
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	var firstRuns, secondRuns atomic.Int32
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- first.Run(ctx1, func(ctx context.Context) error {
			firstRuns.Add(1)
			close(firstStarted)
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first instance did not acquire leadership")
	}
	go func() {
		secondDone <- second.Run(ctx2, func(ctx context.Context) error {
			secondRuns.Add(1)
			close(secondStarted)
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case <-secondStarted:
		t.Fatal("second instance acquired leadership while first held lease")
	case <-time.After(100 * time.Millisecond):
	}
	if firstRuns.Load() != 1 || secondRuns.Load() != 0 {
		t.Fatalf("runs before failover: first=%d second=%d", firstRuns.Load(), secondRuns.Load())
	}
	cancel1()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second instance did not acquire released lease")
	}
	cancel2()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first election loop did not stop")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second election loop did not stop")
	}
}

func TestRunCancelsCallbackWhenLeaseOwnershipChanges(t *testing.T) {
	server := miniredis.RunT(t)
	leader := newTestLeader(t, server, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	cause := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- leader.Run(ctx, func(leaderCtx context.Context) error {
			close(started)
			<-leaderCtx.Done()
			cause <- context.Cause(leaderCtx)
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader callback did not start")
	}
	if err := server.Set(leader.key, "new-owner"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-cause:
		if got != ErrLeadershipLost {
			t.Fatalf("context cause = %v, want %v", got, ErrLeadershipLost)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leadership loss did not cancel the callback")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("election loop did not stop after cancellation")
	}
}

func TestLocalLeaderRunsWithoutRedis(t *testing.T) {
	var called atomic.Bool
	err := (Local{}).Run(context.Background(), func(context.Context) error {
		called.Store(true)
		return nil
	})
	if err != nil || !called.Load() {
		t.Fatalf("local leader called=%t err=%v", called.Load(), err)
	}
}
