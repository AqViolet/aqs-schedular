export type JobStatus = "active" | "paused" | "dead";
export type RunStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "dead"
  | "missed";
export type Job = {
  id: string;
  name: string;
  schedule: string;
  command: string;
  timeout_seconds: number;
  max_retries: number;
  backoff: "exponential" | "linear" | "none";
  status: JobStatus;
  next_run_at: string | null;
  created_at: string | null;
  updated_at: string | null;
};
export type Run = {
  id: string;
  job_id: string;
  status: RunStatus;
  attempt: number;
  scheduled_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  exit_code: number | null;
  duration_ms: number | null;
  error: string | null;
  created_at: string | null;
};
export type Log = {
  run_id: string;
  stream: "stdout" | "stderr";
  chunk: string;
  seq: number;
  created_at: string | null;
};
export type NewJob = Pick<
  Job,
  | "name"
  | "schedule"
  | "command"
  | "timeout_seconds"
  | "max_retries"
  | "backoff"
>;
export type SchedulerStatus = Record<string, unknown>;
