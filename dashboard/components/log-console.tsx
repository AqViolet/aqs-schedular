"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { dateTime, duration } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import type { Log, Run } from "@/lib/types";
import { Badge, Empty, ErrorNotice, Icon, Loading } from "./ui";

export function LogConsole({
  run,
  pending,
  onRetry,
}: {
  run: Run;
  pending: boolean;
  onRetry: () => void;
}) {
  const [follow, setFollow] = useState(true);
  const [stream, setStream] = useState("all");
  const [logLimit, setLogLimit] = useState(1000);
  const details = useResource<Run>(`runs/${encodeURIComponent(run.id)}`, 3000);
  const logs = useResource<Log[]>(
    `runs/${encodeURIComponent(run.id)}/logs?limit=${logLimit}`,
    follow ? 2000 : 0,
  );
  const current = details.data ?? run;
  const viewport = useRef<HTMLDivElement>(null);
  const chunks = useMemo(() => {
    const bySequence = new Map<number, Log>();
    for (const log of logs.data ?? [])
      if (log.run_id === run.id) bySequence.set(log.seq, log);
    return [...bySequence.values()]
      .sort((a, b) => a.seq - b.seq)
      .filter((log) => stream === "all" || log.stream === stream);
  }, [logs.data, run.id, stream]);

  useEffect(() => {
    if (follow && viewport.current)
      viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [chunks, follow]);

  return (
    <section className="log-section" aria-labelledby="output-heading">
      <header className="section-header log-header">
        <div className="section-title">
          <Icon name="terminal" />
          <h3 id="output-heading">Run output</h3>
          <Badge status={current.status} />
        </div>
        <div className="toolbar">
          <button
            className="icon-button"
            title="Refresh output"
            aria-label="Refresh output"
            onClick={() => {
              logs.refresh();
              details.refresh();
            }}
          >
            <Icon name="refresh" size={16} />
          </button>
          {["failed", "dead"].includes(current.status) && (
            <button
              className="button compact"
              disabled={pending}
              onClick={onRetry}
            >
              <Icon name="refresh" size={14} />
              Retry run
            </button>
          )}
        </div>
      </header>
      <dl className="run-metadata">
        <div>
          <dt>Run ID</dt>
          <dd className="mono truncate" title={current.id}>
            {current.id}
          </dd>
        </div>
        <div>
          <dt>Attempt</dt>
          <dd>{current.attempt}</dd>
        </div>
        <div>
          <dt>Duration</dt>
          <dd className="mono">{duration(current.duration_ms)}</dd>
        </div>
        <div>
          <dt>Exit code</dt>
          <dd
            className={
              current.exit_code && current.exit_code !== 0
                ? "text-red mono"
                : "mono"
            }
          >
            {current.exit_code ?? "—"}
          </dd>
        </div>
        <div>
          <dt>Started</dt>
          <dd>{dateTime(current.started_at)}</dd>
        </div>
        <div>
          <dt>Finished</dt>
          <dd>{dateTime(current.finished_at)}</dd>
        </div>
      </dl>
      {current.error && <ErrorNotice message={current.error} />}
      {details.error && (
        <ErrorNotice message={details.error} retry={details.refresh} />
      )}
      {logs.error && <ErrorNotice message={logs.error} retry={logs.refresh} />}
      <div className="console">
        <div className="console-toolbar">
          <div className="console-caption">
            <span className="terminal-dot" />
            <span className="mono">OUTPUT</span>
            <span>{chunks.length} chunks</span>
          </div>
          <div className="toolbar">
            <label className="sr-only" htmlFor="log-stream">
              Output stream
            </label>
            <select
              id="log-stream"
              className="stream-select"
              value={stream}
              onChange={(event) => setStream(event.target.value)}
            >
              <option value="all">All streams</option>
              <option value="stdout">stdout</option>
              <option value="stderr">stderr</option>
            </select>
            {(logs.data?.length ?? 0) >= logLimit && logLimit < 10000 && (
              <button
                className="button compact"
                onClick={() =>
                  setLogLimit((value) => Math.min(10000, value + 1000))
                }
              >
                Load earlier
              </button>
            )}
            <button
              className={`follow-button ${follow ? "following" : ""}`}
              aria-pressed={follow}
              onClick={() => setFollow((value) => !value)}
            >
              <span className="status-dot" />
              {follow ? "Following" : "Follow"}
            </button>
          </div>
        </div>
        <div
          className="console-viewport"
          ref={viewport}
          tabIndex={0}
          aria-label="Run log output"
        >
          {logs.loading ? (
            <Loading label="Fetching output…" />
          ) : !chunks.length ? (
            <Empty
              title={logs.error ? "Output unavailable" : "No output yet"}
              description={
                logs.error
                  ? "Retry the request to load this run’s output."
                  : stream === "all"
                    ? "Output will appear here when the command writes to stdout or stderr."
                    : `No ${stream} output in this run.`
              }
              icon="terminal"
            />
          ) : (
            <pre className="log-output">
              {chunks.map((log) => (
                <span
                  key={log.seq}
                  className={
                    log.stream === "stderr" ? "log-stderr" : "log-stdout"
                  }
                  title={`${log.stream} · #${log.seq} · ${dateTime(log.created_at)}`}
                >
                  {log.chunk}
                </span>
              ))}
            </pre>
          )}
        </div>
        <footer className="console-footer">
          <span>
            {follow
              ? "Polling every 2s · auto-scroll on"
              : "Polling paused · manual refresh available"}
          </span>
          <span>
            {logs.updatedAt
              ? `Updated ${logs.updatedAt.toLocaleTimeString()}`
              : "Waiting for output"}
          </span>
        </footer>
      </div>
    </section>
  );
}
