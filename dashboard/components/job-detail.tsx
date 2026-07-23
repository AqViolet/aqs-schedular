"use client";

import { useState } from "react";
import { dateTime, duration } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import type { Job, Run } from "@/lib/types";
import { LogConsole } from "./log-console";
import { Badge, Empty, ErrorNotice, Icon, Loading, Modal } from "./ui";

export type Mutate = (
  path: string,
  method: "POST" | "DELETE",
  success: string,
) => Promise<boolean>;

export function JobDetail({
  job,
  pending,
  mutationError,
  mutate,
  onDeleted,
}: {
  job: Job;
  pending: boolean;
  mutationError: string | null;
  mutate: Mutate;
  onDeleted: () => void;
}) {
  const runs = useResource<Run[]>(
    `jobs/${encodeURIComponent(job.id)}/runs`,
    5000,
  );
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleteFailed, setDeleteFailed] = useState(false);
  const sortedRuns = [...(runs.data ?? [])].sort(
    (a, b) => Date.parse(b.created_at ?? "") - Date.parse(a.created_at ?? ""),
  );
  const selected =
    sortedRuns.find((run) => run.id === selectedId) ??
    (selectedId ? null : sortedRuns[0]);

  async function action(action: "run" | "pause" | "resume") {
    if (
      await mutate(
        `jobs/${encodeURIComponent(job.id)}/${action}`,
        "POST",
        action === "run"
          ? "Manual run queued."
          : action === "pause"
            ? "Job paused."
            : "Job resumed.",
      )
    ) {
      runs.refresh();
      if (action === "run") setSelectedId(null);
    }
  }

  return (
    <section className="detail-panel" aria-labelledby="job-detail-title">
      <header className="detail-header">
        <div>
          <span className="eyebrow">JOB INSPECTOR</span>
          <div className="detail-heading">
            <h2 id="job-detail-title">{job.name}</h2>
            <Badge status={job.status} />
          </div>
          <p className="mono job-id">{job.id}</p>
        </div>
        <div className="toolbar job-actions">
          <button
            className="button primary compact"
            disabled={pending}
            onClick={() => void action("run")}
          >
            <Icon name="play" size={14} />
            Run now
          </button>
          <button
            className="button compact"
            disabled={pending}
            onClick={() =>
              void action(job.status === "active" ? "pause" : "resume")
            }
          >
            <Icon name={job.status === "active" ? "pause" : "play"} size={14} />
            {job.status === "active" ? "Pause" : "Resume"}
          </button>
          <button
            className="icon-button danger-icon"
            disabled={pending}
            title="Delete job"
            aria-label={`Delete ${job.name}`}
            onClick={() => {
              setDeleteFailed(false);
              setConfirmDelete(true);
            }}
          >
            <Icon name="trash" size={17} />
          </button>
        </div>
      </header>
      <div className="job-command">
        <span className="command-prompt">$</span>
        <code>{job.command}</code>
      </div>
      <dl className="job-metadata">
        <div>
          <dt>Schedule</dt>
          <dd className="mono">{job.schedule}</dd>
        </div>
        <div>
          <dt>Next run</dt>
          <dd>{dateTime(job.next_run_at)}</dd>
        </div>
        <div>
          <dt>Timeout</dt>
          <dd>
            {job.timeout_seconds === 0
              ? "Unlimited"
              : `${job.timeout_seconds}s`}
          </dd>
        </div>
        <div>
          <dt>Attempt limit</dt>
          <dd>
            {job.max_retries} <span className="muted">/ {job.backoff}</span>
          </dd>
        </div>
        <div>
          <dt>Created</dt>
          <dd>{dateTime(job.created_at)}</dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>{dateTime(job.updated_at)}</dd>
        </div>
      </dl>
      <section className="runs-section" aria-labelledby="runs-heading">
        <header className="section-header">
          <div className="section-title">
            <Icon name="clock" />
            <h3 id="runs-heading">Run history</h3>
            <span className="count-pill">{runs.data?.length ?? "—"}</span>
          </div>
          <button
            className="icon-button"
            title="Refresh runs"
            aria-label="Refresh runs"
            onClick={runs.refresh}
          >
            <Icon name="refresh" size={16} />
          </button>
        </header>
        {runs.error && (
          <ErrorNotice message={runs.error} retry={runs.refresh} />
        )}
        {runs.loading ? (
          <Loading label="Loading run history…" />
        ) : !sortedRuns.length ? (
          <Empty
            title={runs.error ? "History unavailable" : "No runs yet"}
            description={
              runs.error
                ? "Retry to load execution history."
                : "Trigger a manual run or wait for the next scheduled execution."
            }
          />
        ) : (
          <div className="table-scroll runs-table-scroll">
            <table className="runs-table">
              <thead>
                <tr>
                  <th scope="col">Run / scheduled</th>
                  <th scope="col">Status</th>
                  <th scope="col">Attempt</th>
                  <th scope="col">Duration</th>
                  <th scope="col">Exit</th>
                </tr>
              </thead>
              <tbody>
                {sortedRuns.map((run) => (
                  <tr
                    key={run.id}
                    className={selected?.id === run.id ? "selected-row" : ""}
                  >
                    <td>
                      <button
                        className="run-select"
                        aria-pressed={selected?.id === run.id}
                        onClick={() => setSelectedId(run.id)}
                      >
                        <span className="mono">{run.id.slice(0, 12)}</span>
                        <span>{dateTime(run.scheduled_at)}</span>
                      </button>
                    </td>
                    <td>
                      <Badge status={run.status} />
                    </td>
                    <td className="mono">{run.attempt}</td>
                    <td className="mono">{duration(run.duration_ms)}</td>
                    <td className="mono">{run.exit_code ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      {selected && (
        <LogConsole
          key={selected.id}
          run={selected}
          pending={pending}
          onRetry={() => {
            void mutate(
              `runs/${encodeURIComponent(selected.id)}/retry`,
              "POST",
              "Retry queued as a new attempt.",
            ).then((ok) => {
              if (ok) {
                runs.refresh();
                setSelectedId(null);
              }
            });
          }}
        />
      )}
      {confirmDelete && (
        <Modal
          title="Delete this job?"
          subtitle="This permanently deletes the job, all its runs, and stored output."
          onClose={() => setConfirmDelete(false)}
          busy={pending}
        >
          <div className="delete-confirm">
            {deleteFailed && mutationError && (
              <ErrorNotice message={mutationError} />
            )}
            <p>
              You are deleting <strong>{job.name}</strong>. This cannot be
              undone.
            </p>
            <p className="muted">
              Deleting a definition does not guarantee an already-running
              process is stopped.
            </p>
          </div>
          <footer className="modal-footer">
            <button
              className="button"
              onClick={() => setConfirmDelete(false)}
              disabled={pending}
            >
              Keep job
            </button>
            <button
              className="button danger"
              disabled={pending}
              onClick={() => {
                void mutate(
                  `jobs/${encodeURIComponent(job.id)}`,
                  "DELETE",
                  "Job deleted.",
                ).then((ok) => {
                  if (ok) onDeleted();
                  else setDeleteFailed(true);
                });
              }}
            >
              <Icon name="trash" size={16} />
              {pending ? "Deleting…" : "Delete permanently"}
            </button>
          </footer>
        </Modal>
      )}
    </section>
  );
}
