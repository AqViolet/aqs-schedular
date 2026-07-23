"use client";

import { useState, type FormEvent } from "react";
import { api, messageOf } from "@/lib/api";
import type { Job, NewJob } from "@/lib/types";
import { ErrorNotice, Icon, Modal } from "./ui";

export function CreateJob({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (job: Job) => void;
}) {
  const [kind, setKind] = useState<"cron" | "once">("cron");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const name = String(form.get("name") ?? "").trim();
    const command = String(form.get("command") ?? "").trim();
    let schedule = String(form.get("schedule") ?? "").trim();
    const timeout = Number(form.get("timeout_seconds"));
    const retries = Number(form.get("max_retries"));
    if (!name || !command || !schedule) {
      setError("Name, schedule, and command are required.");
      return;
    }
    if (
      !Number.isSafeInteger(timeout) ||
      timeout < 0 ||
      !Number.isSafeInteger(retries) ||
      retries < 0
    ) {
      setError("Timeout and retry limit must be non-negative whole numbers.");
      return;
    }
    if (kind === "once") {
      const date = new Date(schedule);
      if (!Number.isFinite(date.getTime()) || date.getTime() <= Date.now()) {
        setError("Choose a one-shot time in the future.");
        return;
      }
      schedule = `once:${date.toISOString()}`;
    }
    const job: NewJob = {
      name,
      command,
      schedule,
      timeout_seconds: timeout,
      max_retries: retries,
      backoff: String(form.get("backoff")) as NewJob["backoff"],
    };
    setBusy(true);
    setError(null);
    try {
      const created = await api<Job>("jobs", {
        method: "POST",
        body: JSON.stringify(job),
      });
      onCreated(created);
    } catch (error) {
      setError(messageOf(error));
      setBusy(false);
    }
  }

  return (
    <Modal
      title="Create a job"
      subtitle="Define what runs, when it runs, and how it recovers."
      onClose={onClose}
      busy={busy}
    >
      <form onSubmit={submit} className="job-form">
        {error && <ErrorNotice message={error} />}
        <fieldset disabled={busy}>
          <label>
            Job name
            <input
              name="name"
              placeholder="nightly-db-backup"
              required
              maxLength={160}
            />
          </label>
          <div className="form-label">Schedule type</div>
          <div className="schedule-toggle">
            <button
              type="button"
              className={kind === "cron" ? "selected" : ""}
              aria-pressed={kind === "cron"}
              onClick={() => setKind("cron")}
            >
              <Icon name="refresh" size={16} />
              Recurring cron
            </button>
            <button
              type="button"
              className={kind === "once" ? "selected" : ""}
              aria-pressed={kind === "once"}
              onClick={() => setKind("once")}
            >
              <Icon name="clock" size={16} />
              One time
            </button>
          </div>
          {kind === "cron" ? (
            <label key="cron">
              Cron expression
              <input
                name="schedule"
                className="mono"
                placeholder="0 2 * * *"
                defaultValue="0 2 * * *"
                required
                aria-describedby="schedule-help"
              />
              <small id="schedule-help">
                Five-field cron, aliases such as @daily, or @every 5m. Uses the
                scheduler host timezone.
              </small>
            </label>
          ) : (
            <label key="once">
              Run at
              <input
                name="schedule"
                type="datetime-local"
                step="1"
                required
                aria-describedby="once-help"
              />
              <small id="once-help">
                Your local time, converted to an RFC3339 UTC timestamp.
              </small>
            </label>
          )}
          <label>
            Shell command
            <textarea
              name="command"
              className="mono"
              rows={4}
              placeholder="sh /opt/scripts/backup.sh"
              required
              maxLength={32000}
              aria-describedby="command-help"
            />
            <small id="command-help">
              Executed through sh -c on the backend host. Only submit commands
              you trust.
            </small>
          </label>
          <div className="form-grid">
            <label>
              Timeout · seconds
              <input
                name="timeout_seconds"
                type="number"
                min="0"
                max="2147483647"
                step="1"
                defaultValue="60"
                required
              />
              <small>0 means no timeout.</small>
            </label>
            <label>
              Retry / attempt limit
              <input
                name="max_retries"
                type="number"
                min="0"
                max="2147483647"
                step="1"
                defaultValue="3"
                required
              />
              <small>Retries while attempt &lt; limit.</small>
            </label>
            <label>
              Backoff
              <select name="backoff" defaultValue="exponential">
                <option value="exponential">Exponential</option>
                <option value="linear">Linear</option>
                <option value="none">None</option>
              </select>
              <small>Delay between attempts.</small>
            </label>
          </div>
        </fieldset>
        <footer className="modal-footer">
          <button
            type="button"
            className="button"
            onClick={onClose}
            disabled={busy}
          >
            Cancel
          </button>
          <button type="submit" className="button primary" disabled={busy}>
            {busy ? (
              <span className="spinner" />
            ) : (
              <Icon name="plus" size={16} />
            )}
            {busy ? "Creating…" : "Create job"}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
