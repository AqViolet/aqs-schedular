package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

var completeAlertScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[2] then return 0 end
local raw = redis.call('HGET', KEYS[2], ARGV[1])
if not raw then return 0 end
local alert = cjson.decode(raw)
local action = ARGV[3]
if action == 'delivered' then
    alert.DeliveredAt = ARGV[4]
elseif action == 'retry' then
    alert.NextAttemptAt = ARGV[4]
elseif action == 'abandon' then
    alert.AbandonedAt = ARGV[4]
else
    return 0
end
alert.LeaseUntil = cjson.null
alert.LastError = ARGV[5]
redis.call('HSET', KEYS[2], ARGV[1], cjson.encode(alert))
redis.call('DEL', KEYS[1])
return 1
`)

func (s *Redis) alertLeaseKey(id string) string { return s.keys[4] + ":lease:" + id }

func (s *Redis) EnqueueAlert(ctx context.Context, a Alert) error {
	a = normalizeAlert(a, time.Now())
	data, err := redisJSON(a)
	if err != nil {
		return err
	}
	_, err = s.client.HSetNX(ctx, s.keys[4], a.ID, data).Result()
	return err
}

func (s *Redis) ClaimAlerts(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Alert, error) {
	if owner == "" || lease < time.Millisecond {
		return nil, errors.New("alert claim requires an owner and lease of at least one millisecond")
	}
	if limit <= 0 {
		limit = 20
	}
	raw, err := s.client.HGetAll(ctx, s.keys[4]).Result()
	if err != nil {
		return nil, err
	}
	var candidates []Alert
	for _, item := range raw {
		var a Alert
		if err := json.Unmarshal([]byte(item), &a); err != nil {
			return nil, err
		}
		if a.DeliveredAt != nil || a.AbandonedAt != nil || a.NextAttemptAt.After(now) {
			continue
		}
		if a.LeaseUntil != nil && a.LeaseUntil.After(now) {
			exists, err := s.client.Exists(ctx, s.alertLeaseKey(a.ID)).Result()
			if err != nil {
				return nil, err
			}
			if exists > 0 {
				continue
			}
		}
		candidates = append(candidates, a)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].NextAttemptAt.Equal(candidates[j].NextAttemptAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].NextAttemptAt.Before(candidates[j].NextAttemptAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	leaseUntil := now.Add(lease).UTC()
	var claimed []Alert
	for _, a := range candidates {
		ok, err := s.client.SetNX(ctx, s.alertLeaseKey(a.ID), owner, lease).Result()
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		a.Attempts++
		a.LeaseUntil = &leaseUntil
		data, err := redisJSON(a)
		if err != nil {
			s.client.Del(ctx, s.alertLeaseKey(a.ID))
			return nil, err
		}
		if err := s.client.HSet(ctx, s.keys[4], a.ID, data).Err(); err != nil {
			s.client.Del(ctx, s.alertLeaseKey(a.ID))
			return nil, err
		}
		claimed = append(claimed, a)
	}
	return claimed, nil
}

func (s *Redis) CompleteAlert(ctx context.Context, id, owner string, now time.Time, result AlertResult) error {
	action, at := "abandon", now
	switch {
	case result.Delivered:
		action = "delivered"
	case result.RetryAt != nil:
		action, at = "retry", *result.RetryAt
	}
	out, err := completeAlertScript.Run(ctx, s.client, []string{s.alertLeaseKey(id), s.keys[4]},
		id, owner, action, redisTime(at), result.LastError).Int64()
	if err != nil {
		return err
	}
	if out != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Redis) AlertCandidates(ctx context.Context, now, since time.Time, statuses []string, limit int) ([]Alert, error) {
	if len(statuses) == 0 || limit <= 0 {
		return nil, nil
	}
	allowed := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		allowed[status] = true
	}
	runsRaw, err := s.client.HGetAll(ctx, s.keys[1]).Result()
	if err != nil {
		return nil, err
	}
	jobsRaw, err := s.client.HGetAll(ctx, s.keys[0]).Result()
	if err != nil {
		return nil, err
	}
	alertsRaw, err := s.client.HGetAll(ctx, s.keys[4]).Result()
	if err != nil {
		return nil, err
	}
	jobs := make(map[string]Job, len(jobsRaw))
	for _, item := range jobsRaw {
		var job Job
		if err := json.Unmarshal([]byte(item), &job); err != nil {
			return nil, err
		}
		jobs[job.ID] = job
	}
	var alerts []Alert
	for _, item := range runsRaw {
		var run Run
		if err := json.Unmarshal([]byte(item), &run); err != nil {
			return nil, err
		}
		if !allowed[run.Status] || run.FinishedAt == nil || run.FinishedAt.Before(since) {
			continue
		}
		job, ok := jobs[run.JobID]
		if !ok {
			continue
		}
		id := run.ID + ":" + run.Status
		if _, exists := alertsRaw[id]; exists {
			continue
		}
		finished := run.FinishedAt.UTC()
		alerts = append(alerts, Alert{
			ID: id, RunID: run.ID, JobID: run.JobID, JobName: job.Name, Status: run.Status,
			Attempt: run.Attempt, ExitCode: run.ExitCode, DurationMS: run.DurationMS, Error: run.Error,
			FinishedAt: &finished, NextAttemptAt: now.UTC(), CreatedAt: now.UTC(),
		})
	}
	sort.Slice(alerts, func(i, j int) bool {
		if alerts[i].FinishedAt.Equal(*alerts[j].FinishedAt) {
			return alerts[i].ID < alerts[j].ID
		}
		return alerts[i].FinishedAt.Before(*alerts[j].FinishedAt)
	})
	if len(alerts) > limit {
		alerts = alerts[:limit]
	}
	return alerts, nil
}

var _ AlertStore = (*Redis)(nil)
