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

export function allowedQuery(path: string[], search: URLSearchParams): boolean {
  if (Array.from(search.keys()).length === 0) return true;
  return (
    path.length === 1 &&
    path[0] === "jobs" &&
    Array.from(search.keys()).length === 1 &&
    search.has("status") &&
    ["active", "paused", "dead"].includes(search.get("status") ?? "")
  );
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
