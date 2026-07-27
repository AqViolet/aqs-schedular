const identifier = /^[A-Za-z0-9_-]{1,128}$/;

export function allowedMethods(path: string[]): string[] {
  if (path.length === 1 && path[0] === "status") return ["GET"];
  if (path.length === 1 && path[0] === "jobs") return ["GET", "POST"];
  if (path.length === 2 && path[0] === "jobs" && identifier.test(path[1]))
    return ["GET", "PATCH", "DELETE"];
  if (path.length === 3 && path[0] === "jobs" && identifier.test(path[1])) {
    if (path[2] === "runs") return ["GET"];
    if (["pause", "resume", "run"].includes(path[2])) return ["POST"];
  }
  if (path.length === 2 && path[0] === "runs" && identifier.test(path[1]))
    return ["GET"];
  if (path.length === 3 && path[0] === "runs" && identifier.test(path[1])) {
    if (path[2] === "logs") return ["GET"];
    if (path[2] === "retry") return ["POST"];
  }
  return [];
}

export function backendBase(value: string): URL {
  const url = new URL(value);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error(
      "AQS_API_URL must be an HTTP(S) origin without credentials, path, query, or fragment.",
    );
  }
  return url;
}

function onlyKeys(search: URLSearchParams, keys: string[]): boolean {
  return (
    Array.from(search.keys()).every((key) => keys.includes(key)) &&
    keys.every((key) => search.getAll(key).length <= 1)
  );
}

function validInt(
  search: URLSearchParams,
  key: string,
  min: number,
  max: number,
): boolean {
  const values = search.getAll(key);
  if (values.length === 0) return true;
  if (values.length !== 1 || !/^\d+$/.test(values[0])) return false;
  const value = Number(values[0]);
  return value >= min && value <= max;
}

function validStatus(search: URLSearchParams, allowed: string[]): boolean {
  const values = search.getAll("status");
  if (values.length === 0) return true;
  return values.length === 1 && allowed.includes(values[0]);
}

export function allowedQuery(path: string[], search: URLSearchParams): boolean {
  if (Array.from(search.keys()).length === 0) return true;
  if (path.length === 1 && path[0] === "jobs") {
    return (
      onlyKeys(search, ["status", "limit", "offset"]) &&
      validStatus(search, ["active", "paused", "dead"]) &&
      validInt(search, "limit", 1, 1000) &&
      validInt(search, "offset", 0, 1_000_000)
    );
  }
  if (path.length === 3 && path[0] === "jobs" && path[2] === "runs") {
    return (
      onlyKeys(search, ["status", "limit", "offset"]) &&
      validStatus(search, [
        "pending",
        "running",
        "succeeded",
        "failed",
        "dead",
        "missed",
      ]) &&
      validInt(search, "limit", 1, 500) &&
      validInt(search, "offset", 0, 1_000_000)
    );
  }
  if (path.length === 3 && path[0] === "runs" && path[2] === "logs") {
    return (
      onlyKeys(search, ["limit", "before", "after"]) &&
      validInt(search, "limit", 1, 10000) &&
      validInt(search, "before", 0, 1_000_000_000) &&
      validInt(search, "after", 0, 1_000_000_000) &&
      !(search.has("before") && search.has("after"))
    );
  }
  return false;
}

function hostOrigin(host: string | null, protocol: string): URL | null {
  if (
    !host ||
    host.length > 255 ||
    !/^[A-Za-z0-9.\-:\[\]]+$/.test(host) ||
    !["http:", "https:"].includes(protocol)
  )
    return null;
  try {
    return new URL(`${protocol}//${host}`);
  } catch {
    return null;
  }
}

export function sameOrigin(
  origin: string | null,
  host: string | null,
  protocol: string,
): boolean {
  const expected = hostOrigin(host, protocol);
  return !!origin && !!expected && origin === expected.origin;
}

export function loopbackHost(host: string | null): boolean {
  const parsed = hostOrigin(host, "http:");
  return (
    !!parsed && ["localhost", "127.0.0.1", "[::1]"].includes(parsed.hostname)
  );
}

export function redact(value: unknown, secrets: string[]): unknown {
  if (typeof value === "string")
    return secrets
      .filter(Boolean)
      .reduce((text, secret) => text.split(secret).join("[redacted]"), value);
  if (Array.isArray(value)) return value.map((item) => redact(item, secrets));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        String(redact(key, secrets)),
        redact(item, secrets),
      ]),
    );
  return value;
}
