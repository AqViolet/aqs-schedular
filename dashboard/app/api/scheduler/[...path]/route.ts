import "server-only";
import { type NextRequest, NextResponse } from "next/server";
import { authorized, authChallenge } from "@/lib/auth";
import {
  allowedMethods,
  allowedQuery,
  backendBase,
  redact,
  sameOrigin,
} from "@/lib/proxy-policy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type Context = { params: Promise<{ path: string[] }> };

function error(
  message: string,
  status: number,
  headers: Record<string, string> = {},
) {
  return NextResponse.json(
    { error: message },
    { status, headers: { "Cache-Control": "no-store", ...headers } },
  );
}

async function limitedText(
  stream: ReadableStream<Uint8Array> | null,
  limit: number,
): Promise<string> {
  if (!stream) return "";
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > limit) {
        await reader.cancel();
        throw new Error("Body too large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function forward(request: NextRequest, context: Context) {
  if (!authorized(request)) return authChallenge();
  const { path } = await context.params;
  const methods = allowedMethods(path);
  if (!methods.length) return error("Unknown scheduler endpoint.", 404);
  if (!methods.includes(request.method))
    return error("Method not allowed.", 405, { Allow: methods.join(", ") });
  if (!allowedQuery(path, request.nextUrl.searchParams))
    return error("Unsupported query parameters.", 400);
  if (request.method !== "GET") {
    const origin = request.headers.get("origin");
    if (
      !sameOrigin(
        origin,
        request.headers.get("host"),
        request.nextUrl.protocol,
      ) ||
      request.headers.get("sec-fetch-site") === "cross-site"
    ) {
      return error("Mutations require a same-origin request.", 403);
    }
  }
  const token = process.env.AQS_API_TOKEN;
  if (!token)
    return error("Set AQS_API_TOKEN in the dashboard server environment.", 503);
  let base: URL;
  try {
    base = backendBase(process.env.AQS_API_URL ?? "http://127.0.0.1:8080");
  } catch {
    return error(
      "AQS_API_URL must be a valid HTTP(S) origin without credentials or a path.",
      503,
    );
  }
  let body: string | undefined;
  if (["POST", "PATCH"].includes(request.method)) {
    try {
      const raw = await limitedText(request.body, 65536);
      if (raw) {
        if (
          !request.headers
            .get("content-type")
            ?.toLowerCase()
            .startsWith("application/json")
        )
          return error("Expected application/json.", 415);
        const parsed: unknown = JSON.parse(raw);
        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object")
          return error("Expected a JSON object.", 400);
        body = JSON.stringify(parsed);
      }
    } catch {
      return error(
        "Request must be valid JSON and no larger than 64 KiB.",
        400,
      );
    }
  }
  const upstream = new URL(`/api/${path.join("/")}`, base);
  upstream.search = request.nextUrl.search;
  try {
    const response = await fetch(upstream, {
      method: request.method,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      body,
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(15000)]),
    });
    if (response.status === 204)
      return new Response(null, {
        status: 204,
        headers: { "Cache-Control": "no-store" },
      });
    if (response.status === 401 || response.status === 403) {
      await response.body?.cancel();
      return error("The scheduler rejected the configured API token.", 502);
    }
    const text = await limitedText(response.body, 16 * 1024 * 1024);
    let payload: unknown;
    try {
      payload = JSON.parse(text);
    } catch {
      return error("The scheduler returned a non-JSON response.", 502);
    }
    return NextResponse.json(
      redact(payload, [token, process.env.DASHBOARD_PASSWORD ?? ""]),
      {
        status: response.status,
        headers: { "Cache-Control": "no-store" },
      },
    );
  } catch {
    return error(
      "Cannot reach the scheduler, the request timed out, or its response was rejected. Check the backend and AQS_API_URL.",
      502,
    );
  }
}

export { forward as GET, forward as POST, forward as PATCH, forward as DELETE };
