import "server-only";
import { createHash, timingSafeEqual } from "node:crypto";
import { loopbackHost } from "./proxy-policy";

export function authorized(request: Request): boolean {
  const password = process.env.DASHBOARD_PASSWORD;
  if (!password) return loopbackHost(request.headers.get("host"));
  const header = request.headers.get("authorization") ?? "";
  if (!/^Basic [A-Za-z0-9+/]+=*$/i.test(header) || header.length > 8192)
    return false;
  const credentials = Buffer.from(header.slice(6), "base64").toString("utf8");
  const actual = createHash("sha256").update(credentials).digest();
  const expected = createHash("sha256").update(`admin:${password}`).digest();
  return timingSafeEqual(actual, expected);
}

export function authChallenge(): Response {
  if (!process.env.DASHBOARD_PASSWORD) {
    return new Response("Non-loopback access requires DASHBOARD_PASSWORD.", {
      status: 403,
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }
  return new Response("Authentication required", {
    status: 401,
    headers: {
      "WWW-Authenticate": 'Basic realm="AQS dashboard", charset="UTF-8"',
      "Cache-Control": "no-store",
      "Content-Type": "text/plain; charset=utf-8",
    },
  });
}
