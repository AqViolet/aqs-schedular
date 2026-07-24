package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
	keys   []string
}

func OpenRedis(url, prefix string) (*Redis, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	if prefix == "" {
		prefix = "aqs-scheduler"
	}
	s := &Redis{client: redis.NewClient(options)}
	for _, suffix := range []string{"jobs", "runs", "logs", "leases", "alerts"} {
		s.keys = append(s.keys, prefix+":"+suffix)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.client.Ping(ctx).Err(); err != nil {
		s.client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return s, nil
}

func (s *Redis) Close() error { return s.client.Close() }

var redisMutation = redis.NewScript(`
local jobs, runs, logs, leases, alerts = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local op, id = ARGV[1], ARGV[2]
local function get(key, field)
    local value = redis.call('HGET', key, field)
    if value then return cjson.decode(value) end
end
local function put(key, field, value)
    redis.call('HSET', key, field, cjson.encode(value))
end
local function servernow()
    local t = redis.call('TIME')
    return tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
end
local function leasekey(runID)
    return leases .. ':' .. runID
end
local function owned(run, lease, owner)
    return run and run.Status == 'running' and lease and lease.owner == owner and lease.serverExpires > servernow() and redis.call('GET', leasekey(run.ID)) == owner
end
if op == 'createJob' then
    if redis.call('HEXISTS', jobs, id) == 1 then return 'duplicate' end
    redis.call('HSET', jobs, id, ARGV[3])
elseif op == 'updateJob' then
    local old = get(jobs, id)
    if not old then return 'missing' end
    local job = cjson.decode(ARGV[3])
    job.CreatedAt = old.CreatedAt
    put(jobs, id, job)
elseif op == 'config' then
    local job = get(jobs, id)
    if not job then return 'missing' end
    local config = cjson.decode(ARGV[3])
    job.Name, job.Schedule, job.Command = config.Name, config.Schedule, config.Command
    job.TimeoutSeconds, job.MaxRetries, job.Backoff = config.TimeoutSeconds, config.MaxRetries, config.Backoff
    if ARGV[4] == '1' then job.NextRunAt = config.NextRunAt end
    job.UpdatedAt = config.UpdatedAt
    put(jobs, id, job)
elseif op == 'status' then
    local job = get(jobs, id)
    if not job then return 'missing' end
    job.Status, job.UpdatedAt = ARGV[3], ARGV[4]
    put(jobs, id, job)
elseif op == 'delete' then
    if redis.call('HEXISTS', jobs, id) == 0 then return 'missing' end
    local all = redis.call('HGETALL', runs)
    local removed = {}
    for i = 1, #all, 2 do
        if cjson.decode(all[i+1]).JobID == id then
            removed[all[i]] = true
            redis.call('HDEL', runs, all[i])
            redis.call('HDEL', logs, all[i])
            redis.call('HDEL', leases, all[i])
            redis.call('DEL', leasekey(all[i]))
        end
    end
    local alertAll = redis.call('HGETALL', alerts)
    for i = 1, #alertAll, 2 do
        local alert = cjson.decode(alertAll[i+1])
        if removed[alert.RunID] then
            redis.call('HDEL', alerts, alertAll[i])
            redis.call('DEL', alerts .. ':lease:' .. alertAll[i])
        end
    end
    redis.call('HDEL', jobs, id)
elseif op == 'createRun' or op == 'schedule' then
    local run = cjson.decode(ARGV[3])
    local job = get(jobs, run.JobID)
    if not job then return 'missing' end
    if op == 'schedule' then
        if run.JobID ~= id then return 'invalid' end
        if job.Status ~= 'active' or job.NextRunAt ~= run.ScheduledAt then return 'ok' end
    end
    if redis.call('HEXISTS', runs, run.ID) == 1 then return 'duplicate' end
    if op == 'schedule' then
        job.NextRunAt = cjson.decode(ARGV[4])
        job.UpdatedAt = ARGV[5]
        put(jobs, id, job)
    end
    redis.call('HSET', runs, run.ID, ARGV[3])
elseif op == 'updateRun' then
    local old = get(runs, id)
    if not old then return 'missing' end
    if redis.call('HEXISTS', leases, id) == 1 then return 'lost' end
    local run = cjson.decode(ARGV[3])
    run.JobID, run.CreatedAt = old.JobID, old.CreatedAt
    put(runs, id, run)
elseif op == 'claim' or op == 'claimLease' then
    local run = get(runs, id)
    if not run or run.Status ~= 'pending' or run.ScheduledAt > ARGV[3] then return 'false' end
    run.Status, run.StartedAt = 'running', ARGV[3]
    if op == 'claimLease' then
        put(leases, id, {owner=ARGV[4], expires=tonumber(ARGV[5]), serverExpires=servernow()+tonumber(ARGV[6])})
                redis.call('PSETEX', leasekey(id), ARGV[6], ARGV[4])
    end
    put(runs, id, run)
elseif op == 'renew' then
    local run, lease = get(runs, id), get(leases, id)
    if not owned(run, lease, ARGV[3]) or lease.expires <= tonumber(ARGV[4]) then return 'false' end
    lease.expires, lease.serverExpires = tonumber(ARGV[5]), servernow()+tonumber(ARGV[6])
    put(leases, id, lease)
    redis.call('PSETEX', leasekey(id), ARGV[6], ARGV[3])
elseif op == 'complete' then
    local old, lease = get(runs, id), get(leases, id)
    if not owned(old, lease, ARGV[3]) then return 'lost' end
    local run = cjson.decode(ARGV[4])
    if run.JobID ~= old.JobID or run.Attempt ~= old.Attempt or (run.Status ~= 'succeeded' and run.Status ~= 'failed' and run.Status ~= 'dead' and run.Status ~= 'interrupted' and run.Status ~= 'missed') then return 'invalid' end
    local retry = cjson.decode(ARGV[5])
    if retry ~= cjson.null then
        if retry.ID == id or retry.JobID ~= old.JobID or retry.Status ~= 'pending' or retry.Attempt ~= old.Attempt+1 then return 'invalid' end
        if redis.call('HEXISTS', runs, retry.ID) == 1 then return 'duplicate' end
    end
    run.CreatedAt = old.CreatedAt
    put(runs, id, run)
    if retry ~= cjson.null then redis.call('HSET', runs, retry.ID, ARGV[5]) end
    redis.call('HDEL', leases, id)
    redis.call('DEL', leasekey(id))
elseif op == 'log' or op == 'leasedLog' then
    local run, lease = get(runs, id), get(leases, id)
    if op == 'leasedLog' then
        if not owned(run, lease, ARGV[3]) then return 'lost' end
    else
        if not run then return 'missing' end
        if lease then return 'lost' end
    end
    local chunks = get(logs, id) or {}
    if chunks[ARGV[4]] then return 'duplicate' end
    chunks[ARGV[4]] = ARGV[5]
    put(logs, id, chunks)
elseif op == 'recover' then
    local all = redis.call('HGETALL', leases)
    local now, server = tonumber(ARGV[3]), servernow()
    for i = 1, #all, 2 do
        local lease = cjson.decode(all[i+1])
        if lease.expires <= now or lease.serverExpires <= server or redis.call('EXISTS', leasekey(all[i])) == 0 then
            local run = get(runs, all[i])
            if run and run.Status == 'running' then
                run.Status, run.Attempt = 'pending', run.Attempt+1
                run.ScheduledAt = ARGV[4]
                run.StartedAt, run.FinishedAt, run.ExitCode, run.DurationMS = cjson.null, cjson.null, cjson.null, cjson.null
                run.Error = 'interrupted: worker lease expired'
                put(runs, all[i], run)
            end
            redis.call('HDEL', leases, all[i])
            redis.call('DEL', leasekey(all[i]))
        end
    end
else
    return 'invalid'
end
return 'ok'
`)

func (s *Redis) mutate(ctx context.Context, args ...any) (bool, error) {
	result, err := redisMutation.Run(ctx, s.client, s.keys, args...).Text()
	if err != nil {
		return false, err
	}
	switch result {
	case "ok":
		return true, nil
	case "false":
		return false, nil
	case "missing":
		return false, sql.ErrNoRows
	case "lost":
		return false, ErrLeaseLost
	default:
		return false, fmt.Errorf("redis store: %s", result)
	}
}

func redisJSON(value any) (string, error) {
	if t, ok := value.(time.Time); ok {
		return strconv.Quote(redisTime(t)), nil
	}
	if r, ok := value.(*Run); ok && r != nil {
		value = *r
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var times map[string]*time.Time
	switch v := value.(type) {
	case Job:
		times = map[string]*time.Time{"CreatedAt": &v.CreatedAt, "UpdatedAt": &v.UpdatedAt, "NextRunAt": v.NextRunAt}
	case Run:
		times = map[string]*time.Time{"CreatedAt": &v.CreatedAt, "ScheduledAt": &v.ScheduledAt, "StartedAt": v.StartedAt, "FinishedAt": v.FinishedAt}
	}
	if times != nil {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return "", err
		}
		for name, t := range times {
			if t != nil {
				fields[name] = json.RawMessage(strconv.Quote(redisTime(*t)))
			}
		}
		data, err = json.Marshal(fields)
	}
	return string(data), err
}

func redisTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000000Z") }

func normalizeJob(j Job) Job {
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	if j.UpdatedAt.IsZero() {
		j.UpdatedAt = j.CreatedAt
	}
	j.CreatedAt, j.UpdatedAt = j.CreatedAt.UTC(), j.UpdatedAt.UTC()
	if j.NextRunAt != nil {
		next := j.NextRunAt.UTC()
		j.NextRunAt = &next
	}
	return j
}

func normalizeRun(r Run) Run {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	r.CreatedAt, r.ScheduledAt = r.CreatedAt.UTC(), r.ScheduledAt.UTC()
	return r
}

func (s *Redis) writeJSON(ctx context.Context, op, id string, value any) error {
	data, err := redisJSON(value)
	if err != nil {
		return err
	}
	_, err = s.mutate(ctx, op, id, data)
	return err
}

func redisGet[T any](ctx context.Context, s *Redis, key, id string) (T, error) {
	var value T
	data, err := s.client.HGet(ctx, key, id).Bytes()
	if errors.Is(err, redis.Nil) {
		return value, sql.ErrNoRows
	}
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(data, &value)
	return value, err
}

func redisList[T any](ctx context.Context, s *Redis, key string) ([]T, error) {
	data, err := s.client.HVals(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var values []T
	for _, item := range data {
		var value T
		if err := json.Unmarshal([]byte(item), &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *Redis) CreateJob(ctx context.Context, j Job) error {
	return s.writeJSON(ctx, "createJob", j.ID, normalizeJob(j))
}
func (s *Redis) GetJob(ctx context.Context, id string) (Job, error) {
	return redisGet[Job](ctx, s, s.keys[0], id)
}
func (s *Redis) UpdateJob(ctx context.Context, j Job) error {
	j.UpdatedAt = time.Now()
	return s.writeJSON(ctx, "updateJob", j.ID, normalizeJob(j))
}
func (s *Redis) UpdateJobConfiguration(ctx context.Context, j Job, scheduleChanged bool) error {
	j.UpdatedAt = time.Now()
	data, err := redisJSON(normalizeJob(j))
	if err != nil {
		return err
	}
	changed := "0"
	if scheduleChanged {
		changed = "1"
	}
	_, err = s.mutate(ctx, "config", j.ID, data, changed)
	return err
}
func (s *Redis) SetJobStatus(ctx context.Context, id, status string) error {
	_, err := s.mutate(ctx, "status", id, status, redisTime(time.Now()))
	return err
}
func (s *Redis) DeleteJob(ctx context.Context, id string) error {
	_, err := s.mutate(ctx, "delete", id)
	return err
}
func (s *Redis) ListJobs(ctx context.Context, status string) ([]Job, error) {
	jobs, err := redisList[Job](ctx, s, s.keys[0])
	if err != nil {
		return nil, err
	}
	var result []Job
	for _, j := range jobs {
		if status == "" || j.Status == status {
			result = append(result, j)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].ID < result[j].ID
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}
func (s *Redis) DueJobs(ctx context.Context, now time.Time) ([]Job, error) {
	jobs, err := s.ListJobs(ctx, "active")
	if err != nil {
		return nil, err
	}
	var result []Job
	for _, j := range jobs {
		if j.NextRunAt != nil && !j.NextRunAt.After(now) {
			result = append(result, j)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].NextRunAt.Before(*result[j].NextRunAt) })
	return result, nil
}
func (s *Redis) CreateRun(ctx context.Context, r Run) error {
	return s.writeJSON(ctx, "createRun", r.ID, normalizeRun(r))
}
func (s *Redis) ScheduleJobRun(ctx context.Context, jobID string, r Run, next *time.Time) error {
	if r.JobID != jobID {
		return errors.New("scheduled run job ID mismatch")
	}
	if next != nil && !next.After(r.ScheduledAt) {
		return errors.New("next run must advance schedule")
	}
	data, err := redisJSON(normalizeRun(r))
	if err != nil {
		return err
	}
	nextJSON := "null"
	if next != nil {
		nextJSON, err = redisJSON(next.UTC())
		if err != nil {
			return err
		}
	}
	_, err = s.mutate(ctx, "schedule", jobID, data, nextJSON, redisTime(time.Now()))
	return err
}
func (s *Redis) GetRun(ctx context.Context, id string) (Run, error) {
	return redisGet[Run](ctx, s, s.keys[1], id)
}
func (s *Redis) UpdateRun(ctx context.Context, r Run) error {
	return s.writeJSON(ctx, "updateRun", r.ID, normalizeRun(r))
}
func (s *Redis) ListRuns(ctx context.Context, jobID string) ([]Run, error) {
	runs, err := redisList[Run](ctx, s, s.keys[1])
	if err != nil {
		return nil, err
	}
	var result []Run
	for _, r := range runs {
		if r.JobID == jobID {
			result = append(result, r)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ScheduledAt.Equal(result[j].ScheduledAt) {
			return result[i].CreatedAt.After(result[j].CreatedAt)
		}
		return result[i].ScheduledAt.After(result[j].ScheduledAt)
	})
	return result, nil
}
func (s *Redis) PendingRuns(ctx context.Context, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = 100
	}
	runs, err := redisList[Run](ctx, s, s.keys[1])
	if err != nil {
		return nil, err
	}
	var result []Run
	now := time.Now()
	for _, r := range runs {
		if r.Status == "pending" && !r.ScheduledAt.After(now) {
			result = append(result, r)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ScheduledAt.Equal(result[j].ScheduledAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ScheduledAt.Before(result[j].ScheduledAt)
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (s *Redis) RunningRuns(ctx context.Context) ([]Run, error) {
	runs, err := redisList[Run](ctx, s, s.keys[1])
	if err != nil {
		return nil, err
	}
	var result []Run
	for _, r := range runs {
		if r.Status == "running" {
			result = append(result, r)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].StartedAt == nil {
			return result[j].StartedAt != nil
		}
		if result[j].StartedAt == nil {
			return false
		}
		return result[i].StartedAt.Before(*result[j].StartedAt)
	})
	return result, nil
}
func (s *Redis) ClaimRun(ctx context.Context, id string, started time.Time) (bool, error) {
	return s.mutate(ctx, "claim", id, redisTime(started))
}
func validateLease(owner string, ttl time.Duration) error {
	if owner == "" || ttl < time.Millisecond {
		return errors.New("lease requires an owner token and TTL of at least one millisecond")
	}
	return nil
}
func (s *Redis) ClaimRunLease(ctx context.Context, id, owner string, now time.Time, ttl time.Duration) (bool, error) {
	if err := validateLease(owner, ttl); err != nil {
		return false, err
	}
	return s.mutate(ctx, "claimLease", id, redisTime(now), owner, now.Add(ttl).UnixMilli(), ttl.Milliseconds())
}
func (s *Redis) RenewRunLease(ctx context.Context, id, owner string, now time.Time, ttl time.Duration) (bool, error) {
	if err := validateLease(owner, ttl); err != nil {
		return false, err
	}
	return s.mutate(ctx, "renew", id, owner, now.UnixMilli(), now.Add(ttl).UnixMilli(), ttl.Milliseconds())
}
func (s *Redis) CompleteRun(ctx context.Context, r Run, owner string, retry *Run) error {
	data, err := redisJSON(normalizeRun(r))
	if err != nil {
		return err
	}
	if retry != nil {
		normalized := normalizeRun(*retry)
		retry = &normalized
	}
	retryJSON, err := redisJSON(retry)
	if err != nil {
		return err
	}
	_, err = s.mutate(ctx, "complete", r.ID, owner, data, retryJSON)
	return err
}
func (s *Redis) appendLog(ctx context.Context, op, owner string, l Log) error {
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}
	data, err := redisJSON(l)
	if err != nil {
		return err
	}
	_, err = s.mutate(ctx, op, l.RunID, owner, strconv.FormatInt(l.Seq, 10), data)
	return err
}
func (s *Redis) AppendLog(ctx context.Context, l Log) error { return s.appendLog(ctx, "log", "", l) }
func (s *Redis) AppendRunLog(ctx context.Context, owner string, l Log) error {
	return s.appendLog(ctx, "leasedLog", owner, l)
}
func (s *Redis) ListLogs(ctx context.Context, runID string) ([]Log, error) {
	chunks, err := redisGet[map[string]string](ctx, s, s.keys[2], runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var logs []Log
	for _, chunk := range chunks {
		var l Log
		if err := json.Unmarshal([]byte(chunk), &l); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].Seq < logs[j].Seq })
	return logs, nil
}
func (s *Redis) RecoverExpiredRuns(ctx context.Context, now time.Time) error {
	_, err := s.mutate(ctx, "recover", "", now.UnixMilli(), redisTime(now))
	return err
}

var _ DistributedStore = (*Redis)(nil)
var _ JobConfigurationStore = (*Redis)(nil)
