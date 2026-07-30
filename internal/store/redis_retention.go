package store

import (
	"context"
	"encoding/json"
	"time"
)

func (s *Redis) PruneRuns(ctx context.Context, before time.Time, statuses []string, dryRun bool) (int64, error) {
	statuses, err := ValidatePruneStatuses(statuses)
	if err != nil {
		return 0, err
	}
	allowed := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		allowed[status] = true
	}
	runsRaw, err := s.client.HGetAll(ctx, s.keys[1]).Result()
	if err != nil {
		return 0, err
	}
	alertsRaw, err := s.client.HGetAll(ctx, s.keys[4]).Result()
	if err != nil {
		return 0, err
	}
	alertsByRun := make(map[string][]string)
	pending := make(map[string]bool)
	for field, raw := range alertsRaw {
		var alert Alert
		if err := json.Unmarshal([]byte(raw), &alert); err != nil {
			return 0, err
		}
		alertsByRun[alert.RunID] = append(alertsByRun[alert.RunID], field)
		if alert.DeliveredAt == nil && alert.AbandonedAt == nil {
			pending[alert.RunID] = true
		}
	}
	var ids []string
	for _, raw := range runsRaw {
		var run Run
		if err := json.Unmarshal([]byte(raw), &run); err != nil {
			return 0, err
		}
		at := run.ScheduledAt
		if run.FinishedAt != nil {
			at = *run.FinishedAt
		}
		if !allowed[run.Status] || !at.Before(before) || pending[run.ID] {
			continue
		}
		ids = append(ids, run.ID)
	}
	if dryRun {
		return int64(len(ids)), nil
	}
	pipe := s.client.Pipeline()
	for _, id := range ids {
		pipe.HDel(ctx, s.keys[1], id)
		pipe.HDel(ctx, s.keys[2], id)
		pipe.HDel(ctx, s.keys[3], id)
		pipe.Del(ctx, s.keys[3]+":"+id)
		for _, field := range alertsByRun[id] {
			pipe.HDel(ctx, s.keys[4], field)
			pipe.Del(ctx, s.alertLeaseKey(field))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}

var _ RetentionStore = (*Redis)(nil)
