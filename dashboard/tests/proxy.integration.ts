import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer, type Server } from "node:http";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";

const token = "integration-backend-token";
const password = "integration-dashboard-password";
const basic = `Basic ${Buffer.from(`admin:${password}`).toString("base64")}`;
const received: {
  path: string;
  method: string;
  authorization?: string;
  cookie?: string;
  body: string;
}[] = [];
let backend: Server;
let child: ChildProcess | undefined;
let origin = "";
let output = "";

async function listen(server: Server): Promise<number> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return address.port;
}

async function request(path: string, options: RequestInit = {}) {
  return fetch(`${origin}${path}`, {
    ...options,
    headers: { Authorization: basic, ...options.headers },
    signal: AbortSignal.timeout(10000),
    redirect: "manual",
  });
}

before(
  async () => {
    backend = createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      received.push({
        path: req.url ?? "",
        method: req.method ?? "",
        authorization: req.headers.authorization,
        cookie: req.headers.cookie,
        body: Buffer.concat(chunks).toString(),
      });
      if (req.url === "/api/jobs/redirect") {
        res.writeHead(302, { Location: "/api/redirect-target" });
        res.end();
        return;
      }
      if (req.url === "/api/jobs/nonjson") {
        res.writeHead(500);
        res.end("not JSON");
        return;
      }
      if (req.url === "/api/jobs/rejected") {
        res.writeHead(401, { "WWW-Authenticate": "Bearer" });
        res.end(JSON.stringify({ error: token }));
        return;
      }
      if (req.method === "DELETE") {
        res.writeHead(204);
        res.end();
        return;
      }
      res.writeHead(
        req.method === "POST" && req.url === "/api/jobs" ? 201 : 200,
        {
          "Content-Type": "application/json",
          "X-Backend-Secret": token,
          "Set-Cookie": "upstream=secret",
        },
      );
      if (req.url === "/api/jobs/secret") {
        res.end(JSON.stringify({ nested: [token, password] }));
        return;
      }
      res.end(JSON.stringify({ ok: true }));
    });
    const backendPort = await listen(backend);
    const reservation = createServer();
    const dashboardPort = await listen(reservation);
    await new Promise<void>((resolve) => reservation.close(() => resolve()));
    origin = `http://127.0.0.1:${dashboardPort}`;
    child = spawn(
      process.execPath,
      [
        "node_modules/next/dist/bin/next",
        "start",
        "--hostname",
        "127.0.0.1",
        "--port",
        String(dashboardPort),
      ],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          AQS_API_URL: `http://127.0.0.1:${backendPort}`,
          AQS_API_TOKEN: token,
          DASHBOARD_PASSWORD: password,
          NEXT_TELEMETRY_DISABLED: "1",
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    child.stdout?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    for (let attempt = 0; attempt < 100; attempt++) {
      if (child.exitCode !== null) throw new Error(`Next.js exited: ${output}`);
      try {
        const response = await fetch(origin, {
          signal: AbortSignal.timeout(1000),
        });
        if (response.status === 401) return;
      } catch {}
      await delay(200);
    }
    throw new Error(`Next.js startup timed out: ${output}`);
  },
  { timeout: 30000 },
);

after(async () => {
  if (child && child.exitCode === null) {
    const exited = once(child, "exit");
    child.kill("SIGTERM");
    const stopped = await Promise.race([
      exited.then(() => true),
      delay(5000).then(() => false),
    ]);
    if (!stopped) {
      child.kill("SIGKILL");
      await exited;
    }
  }
  if (backend?.listening)
    await new Promise<void>((resolve) => backend.close(() => resolve()));
});

test("protects pages and API with configured Basic auth", async () => {
  for (const path of ["/", "/api/scheduler/jobs"]) {
    for (const authorization of [
      "",
      `Basic ${Buffer.from("admin:wrong").toString("base64")}`,
      "Bearer browser-token",
    ]) {
      const response = await fetch(`${origin}${path}`, {
        headers: { Authorization: authorization },
      });
      assert.equal(response.status, 401);
      assert.match(response.headers.get("www-authenticate") ?? "", /^Basic /);
      assert.equal(response.headers.get("cache-control"), "no-store");
    }
  }
  const response = await request("/");
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /Keep work moving/);
  assert.ok(!html.includes(token));
  assert.ok(!html.includes(password));
});

test("forwards the allowlisted API with server bearer token, never browser credentials/cookies", async () => {
  const response = await request("/api/scheduler/jobs?status=active", {
    headers: { Cookie: "browser=private" },
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { ok: true });
  assert.deepEqual(received.at(-1), {
    path: "/api/jobs?status=active",
    method: "GET",
    authorization: `Bearer ${token}`,
    cookie: undefined,
    body: "",
  });
  assert.equal(response.headers.get("x-backend-secret"), null);
  assert.equal(response.headers.get("set-cookie"), null);
  assert.equal(response.headers.get("cache-control"), "no-store");
});

test("forwards bounded pagination queries unchanged", async () => {
  for (const path of [
    "/api/scheduler/jobs?limit=10&offset=20",
    "/api/scheduler/jobs/job-1/runs?status=failed&limit=25&offset=50",
    "/api/scheduler/runs/run-1/logs?limit=200&before=900",
    "/api/scheduler/runs/run-1/logs?after=10",
  ]) {
    const response = await request(path);
    assert.equal(response.status, 200);
    assert.equal(received.at(-1)?.path, path.replace("/api/scheduler", "/api"));
  }
});

test("rejects non-allowlisted paths, methods, queries, and cross-origin mutations before forwarding", async () => {
  const count = received.length;
  for (const [path, options, expected] of [
    ["/api/scheduler/admin", {}, 404],
    ["/api/scheduler/jobs/bad%2Fid", {}, 404],
    ["/api/scheduler/jobs?url=http://evil.test", {}, 400],
    ["/api/scheduler/jobs?status=active&status=dead", {}, 400],
    ["/api/scheduler/jobs?limit=0", {}, 400],
    ["/api/scheduler/runs/run-1/logs?before=1&after=2", {}, 400],
    [
      "/api/scheduler/status",
      { method: "POST", headers: { Origin: origin } },
      405,
    ],
    ["/api/scheduler/jobs", { method: "POST" }, 403],
    [
      "/api/scheduler/jobs",
      { method: "POST", headers: { Origin: "https://evil.test" } },
      403,
    ],
    [
      "/api/scheduler/jobs",
      {
        method: "POST",
        headers: { Origin: origin, "Sec-Fetch-Site": "cross-site" },
      },
      403,
    ],
    [
      "/api/scheduler/jobs",
      {
        method: "POST",
        headers: { Origin: origin, "Content-Type": "application/json" },
        body: "invalid",
      },
      400,
    ],
    [
      "/api/scheduler/jobs",
      {
        method: "POST",
        headers: { Origin: origin, "Content-Type": "application/json" },
        body: JSON.stringify({ command: "x".repeat(65536) }),
      },
      400,
    ],
  ] as [string, RequestInit, number][]) {
    const response = await request(path, options);
    assert.equal(
      response.status,
      expected,
      `${path} ${JSON.stringify(options.headers)}`,
    );
  }
  assert.equal(received.length, count);
});

test("preserves successful creation, patching, actions, and empty deletion", async () => {
  const job = {
    name: "test",
    schedule: "@daily",
    command: "echo test",
    timeout_seconds: 60,
    max_retries: 3,
    backoff: "exponential",
  };
  const created = await request("/api/scheduler/jobs", {
    method: "POST",
    headers: { Origin: origin, "Content-Type": "application/json" },
    body: JSON.stringify(job),
  });
  assert.equal(created.status, 201);
  assert.deepEqual(JSON.parse(received.at(-1)?.body ?? ""), job);
  const patched = await request("/api/scheduler/jobs/job-1", {
    method: "PATCH",
    headers: { Origin: origin, "Content-Type": "application/json" },
    body: JSON.stringify({ name: "updated" }),
  });
  assert.equal(patched.status, 200);
  for (const path of [
    "jobs/job-1/pause",
    "jobs/job-1/resume",
    "jobs/job-1/run",
    "runs/run-1/retry",
  ]) {
    const response = await request(`/api/scheduler/${path}`, {
      method: "POST",
      headers: { Origin: origin },
    });
    assert.equal(response.status, 200);
    assert.equal(received.at(-1)?.path, `/api/${path}`);
  }
  const deleted = await request("/api/scheduler/jobs/job-1", {
    method: "DELETE",
    headers: { Origin: origin },
  });
  assert.equal(deleted.status, 204);
  assert.equal(await deleted.text(), "");
});

test("rejects redirects/non-JSON and translates backend auth failures without exposing headers or secrets", async () => {
  for (const id of ["redirect", "nonjson", "rejected"]) {
    const response = await request(`/api/scheduler/jobs/${id}`);
    assert.equal(response.status, 502);
    assert.equal(response.headers.get("location"), null);
    assert.equal(response.headers.get("www-authenticate"), null);
    const text = await response.text();
    assert.ok(!text.includes(token));
  }
  assert.ok(!received.some((entry) => entry.path === "/api/redirect-target"));
  const response = await request("/api/scheduler/jobs/secret");
  assert.deepEqual(await response.json(), {
    nested: ["[redacted]", "[redacted]"],
  });
});
