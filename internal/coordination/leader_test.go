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

func TestRenewalContinuesWhileGracefulShutdownDrains(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	var renewals atomic.Int32
	leader := &RedisLeader{
		client: client, key: "drain:leader", token: "owner",
		ttl: 300 * time.Millisecond, retry: time.Millisecond,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		renewEvery: 10 * time.Millisecond,
		onRenew:    func() { renewals.Add(1) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	drained := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- leader.Run(ctx, func(leaderCtx context.Context) error {
			close(started)
			<-leaderCtx.Done()
			// Simulate a local node draining in-flight work after shutdown.
			time.Sleep(150 * time.Millisecond)
			close(drained)
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader callback did not start")
	}
	before := renewals.Load()
	cancel()
	deadline := time.Now().Add(time.Second)
	for renewals.Load() <= before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if renewals.Load() <= before {
		t.Fatal("lease renewal stopped during graceful drain")
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain did not finish")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("election loop did not stop")
	}
	if value, err := client.Get(context.Background(), leader.key).Result(); err != redis.Nil || value != "" {
		t.Fatalf("lease was not released: value=%q err=%v", value, err)
	}
}

func TestLeadershipLossDoesNotPropagateCallbackCancellation(t *testing.T) {
	server := miniredis.RunT(t)
	leader := newTestLeader(t, server, 10*time.Millisecond)
	leader.ttl = 300 * time.Millisecond
	leader.renewEvery = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- leader.Run(ctx, func(leaderCtx context.Context) error {
			close(started)
			<-leaderCtx.Done()
			// Returning the cancellation error must not be mistaken for a
			// service failure when the cause was leadership loss.
			return leaderCtx.Err()
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
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("leadership loss returned an error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("election loop did not stop")
	}
}
