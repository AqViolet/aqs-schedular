"use client";

import { useRef, useState } from "react";
import Link from "next/link";
import { api, dateTime, messageOf, statusCount } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import type { Job, JobStatus, SchedulerStatus } from "@/lib/types";
import { CreateJob } from "./create-job";
import { JobDetail } from "./job-detail";
import { Badge, Empty, ErrorNotice, Icon, Loading } from "./ui";

type Filter = "all" | JobStatus;

export function Dashboard() {
  const [jobLimit, setJobLimit] = useState(200);
  const jobs = useResource<Job[]>(`jobs?limit=${jobLimit}`, 5000);
  const status = useResource<SchedulerStatus>("status", 5000);
  const [filter, setFilter] = useState<Filter>("all");
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const mutationLock = useRef(false);
  const [notice, setNotice] = useState<{
    kind: "success" | "error";
    message: string;
  } | null>(null);
  const allJobs = jobs.data ?? [];
  const counts = { all: allJobs.length, active: 0, paused: 0, dead: 0 };
  for (const job of allJobs) if (job.status in counts) counts[job.status]++;
  const filtered = allJobs
    .filter(
      (job) =>
        (filter === "all" || job.status === filter) &&
        `${job.name} ${job.command} ${job.id} ${job.schedule}`
          .toLowerCase()
          .includes(query.toLowerCase()),
    )
    .sort(
      (a, b) => Date.parse(b.created_at ?? "") - Date.parse(a.created_at ?? ""),
    );
  const selected = allJobs.find((job) => job.id === selectedId);
  const connected = !!status.data && !status.error;

  function refresh() {
    jobs.refresh();
    status.refresh();
  }

  async function mutate(
    path: string,
    method: "POST" | "DELETE",
    success: string,
  ) {
    if (mutationLock.current) return false;
    mutationLock.current = true;
    setPending(true);
    setNotice(null);
    try {
      await api<unknown>(path, { method });
      setNotice({ kind: "success", message: success });
      refresh();
      return true;
    } catch (error) {
      setNotice({ kind: "error", message: messageOf(error) });
      return false;
    } finally {
      mutationLock.current = false;
      setPending(false);
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <Link className="brand" href="/" aria-label="AQS scheduler home">
          <span className="brand-mark">
            <span />
            <span />
            <span />
          </span>
          <span>
            AQS<span className="brand-subtitle">SCHEDULER</span>
          </span>
        </Link>
        <div className="sidebar-label">WORKSPACE</div>
        <a className="nav-item current" href="#overview">
          <Icon name="grid" />
          <span>Operations</span>
          <span className="nav-count">{jobs.data ? counts.all : "—"}</span>
        </a>
        <a className="nav-item" href="#jobs">
          <Icon name="clock" />
          <span>Scheduled jobs</span>
        </a>
        <div className="sidebar-note">
          <span className="eyebrow">EXECUTION MODEL</span>
          <p>
            Durable schedules.
            <br />
            Observable runs.
          </p>
          <span className="note-line" />
        </div>
        <footer className="sidebar-footer">
          <span className="local-mark">L</span>
          <div>
            Local workspace<small>Administrator console</small>
          </div>
          <span className="status-dot" />
        </footer>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            Workspace <span>/</span>
            <strong>Operations</strong>
          </div>
          <div className="connection">
            <span
              className={`status-dot ${connected ? "connected" : status.loading ? "connecting" : "disconnected"}`}
            />
            {status.loading
              ? "Connecting"
              : connected
                ? "API connected"
                : "API unavailable"}
            <span className="environment-tag">LOCAL / ADMIN</span>
          </div>
        </header>
        <main id="overview">
          <div className="page-heading">
            <div>
              <span className="eyebrow">SCHEDULER CONTROL PLANE</span>
              <h1>
                Keep work moving<span className="accent">.</span>
              </h1>
              <p>
                Schedule commands. Inspect every attempt. Recover with
                confidence.
              </p>
            </div>
            <div className="heading-actions">
              <button className="button" onClick={refresh} disabled={pending}>
                <Icon name="refresh" size={16} />
                Refresh
              </button>
              <button
                className="button primary"
                onClick={() => setCreateOpen(true)}
                disabled={pending}
              >
                <Icon name="plus" size={17} />
                Create job
              </button>
            </div>
          </div>
          {notice && (
            <div
              className={`notice notice-${notice.kind}`}
              role={notice.kind === "error" ? "alert" : "status"}
            >
              <div>
                {notice.kind === "success" && <Icon name="check" size={17} />}
                <span>{notice.message}</span>
              </div>
              <button
                className="icon-button"
                aria-label="Dismiss notification"
                onClick={() => setNotice(null)}
              >
                <Icon name="close" size={16} />
              </button>
            </div>
          )}
          {status.error && (
            <ErrorNotice
              message={`Status: ${status.error}`}
              retry={status.refresh}
            />
          )}
          <section className="overview-grid" aria-label="Job overview">
            {(
              [
                ["all", "Total jobs", "Every defined workload", "grid"],
                ["active", "Active", "Ready for scheduled execution", "play"],
                ["paused", "Paused", "Scheduling on hold", "pause"],
                ["dead", "Dead jobs", "Inspect before resuming", "terminal"],
              ] as const
            ).map(([key, label, caption, icon]) => (
              <button
                className={`stat-card stat-${key} ${filter === key ? "stat-selected" : ""}`}
                key={key}
                aria-pressed={filter === key}
                onClick={() => {
                  setFilter(key);
                  document
                    .getElementById("jobs")
                    ?.scrollIntoView({ behavior: "smooth", block: "nearest" });
                }}
              >
                <span className="stat-top">
                  <span>{label}</span>
                  <Icon name={icon} size={17} />
                </span>
                <span className="stat-value">
                  {jobs.data ? counts[key] : "—"}
                </span>
                <span className="stat-caption">
                  <span className="status-dot" />
                  {caption}
                </span>
              </button>
            ))}
          </section>
          <div className="activity-strip">
            <span>
              <span
                className={`status-dot ${connected ? "connected" : "disconnected"}`}
              />
              {connected
                ? "Scheduler API reachable"
                : "Awaiting scheduler connection"}
            </span>
            <div>
              <span>
                <strong>
                  {statusCount(status.data, "runs", "running") ?? "—"}
                </strong>{" "}
                running
              </span>
              <span>
                <strong>
                  {statusCount(status.data, "runs", "pending") ?? "—"}
                </strong>{" "}
                pending
              </span>
              <span className="sync-time">
                {jobs.updatedAt
                  ? `Synced ${jobs.updatedAt.toLocaleTimeString()}`
                  : "Sync pending"}
              </span>
            </div>
          </div>
          <div className={`workspace ${selected ? "has-selection" : ""}`}>
            <section
              className="jobs-panel"
              id="jobs"
              aria-labelledby="jobs-heading"
            >
              <header className="section-header">
                <div className="section-title">
                  <h2 id="jobs-heading">Jobs</h2>
                  <span className="count-pill">
                    {jobs.data ? counts.all : "—"}
                  </span>
                </div>
                <span className="subtle-label">AUTO-SYNC · 5s</span>
              </header>
              <div className="jobs-toolbar">
                <div className="filter-tabs" aria-label="Filter jobs">
                  {(["all", "active", "paused", "dead"] as const).map(
                    (value) => (
                      <button
                        key={value}
                        aria-pressed={filter === value}
                        className={filter === value ? "active" : ""}
                        onClick={() => setFilter(value)}
                      >
                        {value === "all" ? "All jobs" : value}
                        <span>{jobs.data ? counts[value] : "—"}</span>
                      </button>
                    ),
                  )}
                </div>
                <label className="search-input">
                  <Icon name="search" size={16} />
                  <span className="sr-only">Search jobs</span>
                  <input
                    type="search"
                    placeholder="Search jobs or commands…"
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                  />
                </label>
              </div>
              {jobs.error && (
                <ErrorNotice message={jobs.error} retry={jobs.refresh} />
              )}
              {jobs.loading ? (
                <Loading label="Loading scheduled jobs…" />
              ) : !filtered.length ? (
                <>
                  <Empty
                    title={
                      jobs.error && !jobs.data
                        ? "Jobs unavailable"
                        : allJobs.length
                          ? "No matching jobs"
                          : "Your first job starts here"
                    }
                    description={
                      jobs.error && !jobs.data
                        ? "Check your backend connection and server environment, then retry."
                        : allJobs.length
                          ? "Try a different status or search term."
                          : "Create a recurring task or a one-shot command to bring this workspace to life."
                    }
                    icon={allJobs.length ? "search" : "clock"}
                  />
                  {!allJobs.length && !jobs.error && (
                    <div className="empty-action">
                      <button
                        className="button primary"
                        onClick={() => setCreateOpen(true)}
                      >
                        <Icon name="plus" size={16} />
                        Create your first job
                      </button>
                    </div>
                  )}
                </>
              ) : (
                <div className="table-scroll">
                  <table className="jobs-table">
                    <thead>
                      <tr>
                        <th scope="col">Job / command</th>
                        <th scope="col">Status</th>
                        <th scope="col">Schedule</th>
                        <th scope="col">Next run</th>
                        <th scope="col">
                          <span className="sr-only">Inspect</span>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {filtered.map((job) => (
                        <tr
                          key={job.id}
                          className={
                            selectedId === job.id ? "selected-row" : ""
                          }
                        >
                          <td>
                            <button
                              className="job-select"
                              aria-pressed={selectedId === job.id}
                              onClick={() => setSelectedId(job.id)}
                            >
                              <span className="job-name">
                                <span className="job-glyph">
                                  <Icon name="terminal" size={16} />
                                </span>
                                {job.name}
                              </span>
                              <span
                                className="job-list-command mono"
                                title={job.command}
                              >
                                {job.command}
                              </span>
                            </button>
                          </td>
                          <td>
                            <Badge status={job.status} />
                          </td>
                          <td
                            className="mono schedule-cell"
                            title={job.schedule}
                          >
                            {job.schedule}
                          </td>
                          <td className="next-run-cell">
                            {dateTime(job.next_run_at)}
                          </td>
                          <td>
                            <button
                              className="icon-button"
                              aria-label={`Inspect ${job.name}`}
                              onClick={() => setSelectedId(job.id)}
                            >
                              <Icon name="chevron" size={16} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {allJobs.length >= jobLimit && jobLimit < 1000 && (
                <div className="empty-action">
                  <button
                    className="button compact"
                    onClick={() => setJobLimit((value) => Math.min(1000, value + 200))}
                  >
                    Load more jobs
                  </button>
                </div>
              )}
              <footer className="panel-footer">
                <span>
                  {filtered.length} of {allJobs.length} jobs
                  {allJobs.length >= jobLimit ? " (bounded view)" : ""}
                </span>
                <span>Times shown in your local timezone</span>
              </footer>
            </section>
            {selected ? (
              <JobDetail
                key={selected.id}
                job={selected}
                pending={pending}
                mutationError={notice?.kind === "error" ? notice.message : null}
                mutate={mutate}
                onDeleted={() => setSelectedId(null)}
              />
            ) : (
              <section className="inspector-placeholder">
                <div className="inspector-art">
                  <span />
                  <Icon name="terminal" size={28} />
                  <span />
                </div>
                <span className="eyebrow">FROM SCHEDULE TO STDOUT</span>
                <h2>Every run tells a story.</h2>
                <p>
                  Select a job to inspect its configuration, browse execution
                  history, and follow live output.
                </p>
                <span className="inspector-hint">
                  <Icon name="arrow" size={16} />
                  Select any job to get started
                </span>
              </section>
            )}
          </div>
          <footer className="page-footer">
            <span>
              AQS <span className="muted">/</span> Scheduler operations
            </span>
            <span>Shell execution · Local admin access only</span>
          </footer>
        </main>
      </div>
      {createOpen && (
        <CreateJob
          onClose={() => setCreateOpen(false)}
          onCreated={(job) => {
            setCreateOpen(false);
            setSelectedId(job.id);
            setFilter("all");
            setQuery("");
            setNotice({ kind: "success", message: `Created ${job.name}.` });
            refresh();
          }}
        />
      )}
    </div>
  );
}
