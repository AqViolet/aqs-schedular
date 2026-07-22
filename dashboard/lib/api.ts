import type { SchedulerStatus } from "./types";

export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(`/api/scheduler/${path}`, {
    ...options,
    cache: "no-store",
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  if (response.status === 204) return undefined as T;
  const payload: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const message =
      payload &&
      typeof payload === "object" &&
      "error" in payload &&
      typeof payload.error === "string"
        ? payload.error
        : payload &&
            typeof payload === "object" &&
            "message" in payload &&
            typeof payload.message === "string"
          ? payload.message
          : `Request failed (${response.status}).`;
    throw new Error(message);
  }
  if (payload === null)
    throw new Error("The scheduler returned an empty or invalid response.");
  return payload as T;
}

export function messageOf(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "An unexpected error occurred.";
}

export function statusCount(
  status: SchedulerStatus | null,
  group: "jobs" | "runs",
  state: string,
): number | null {
  if (!status) return null;
  for (const key of [
    group,
    `${group}_by_status`,
    `${group.slice(0, -1)}_counts`,
  ]) {
    const counts = status[key];
    if (counts && typeof counts === "object" && state in counts) {
      const value = (counts as Record<string, unknown>)[state];
      if (typeof value === "number") return value;
    }
  }
  const value = status[`${state}_${group}`];
  return typeof value === "number" ? value : null;
}

export function dateTime(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function duration(value: number | null): string {
  if (value === null) return "—";
  return value < 1000 ? `${value} ms` : `${(value / 1000).toFixed(1)} s`;
}
