import assert from "node:assert/strict";
import { test } from "node:test";
import {
  allowedMethods,
  allowedQuery,
  backendBase,
  loopbackHost,
  redact,
  sameOrigin,
} from "../lib/proxy-policy.ts";

const endpoints: [string[], string[]][] = [
  [["jobs"], ["GET", "POST"]],
  [
    ["jobs", "job-1"],
    ["GET", "PATCH", "DELETE"],
  ],
  [["jobs", "job-1", "pause"], ["POST"]],
  [["jobs", "job-1", "resume"], ["POST"]],
  [["jobs", "job-1", "run"], ["POST"]],
  [["jobs", "job-1", "runs"], ["GET"]],
  [["runs", "run_1"], ["GET"]],
  [["runs", "run_1", "logs"], ["GET"]],
  [["runs", "run_1", "retry"], ["POST"]],
  [["status"], ["GET"]],
];

for (const [path, methods] of endpoints) {
  test(`allowlists ${path.join("/")}`, () =>
    assert.deepEqual(allowedMethods(path), methods));
}

test("rejects arbitrary endpoints, traversal, URLs, invalid IDs, and extra segments", () => {
  for (const path of [
    [],
    ["admin"],
    ["jobs", ".."],
    ["jobs", "."],
    ["jobs", "a/b"],
    ["jobs", "%2F"],
    ["jobs", "https://evil.test"],
    ["jobs", "x?url=evil"],
    ["jobs", "x", "delete"],
    ["runs", "x", "logs", "extra"],
    ["status", "extra"],
    ["jobs", "a".repeat(129)],
  ]) {
    assert.deepEqual(allowedMethods(path), [], path.join("/"));
  }
});

test("permits only documented pagination and status queries", () => {
  assert.equal(allowedQuery(["jobs"], new URLSearchParams()), true);
  for (const state of ["active", "paused", "dead"])
    assert.equal(
      allowedQuery(["jobs"], new URLSearchParams({ status: state })),
      true,
    );
  assert.equal(allowedQuery(["jobs"], new URLSearchParams("limit=10")), true);
  assert.equal(
    allowedQuery(["jobs"], new URLSearchParams("offset=20&limit=10")),
    true,
  );
  for (const query of [
    "url=https://evil.test",
    "status=unknown",
    "status=active&status=dead",
    "status=active&token=secret",
    "limit=0",
    "limit=1001",
    "offset=-1",
    "limit=10&limit=20",
  ]) {
    assert.equal(allowedQuery(["jobs"], new URLSearchParams(query)), false);
  }
  assert.equal(
    allowedQuery(
      ["jobs", "job-1", "runs"],
      new URLSearchParams("status=failed&limit=25&offset=50"),
    ),
    true,
  );
  assert.equal(
    allowedQuery(["jobs", "job-1", "runs"], new URLSearchParams("limit=501")),
    false,
  );
  assert.equal(
    allowedQuery(
      ["runs", "run-1", "logs"],
      new URLSearchParams("limit=200&before=900"),
    ),
    true,
  );
  assert.equal(
    allowedQuery(
      ["runs", "run-1", "logs"],
      new URLSearchParams("after=10"),
    ),
    true,
  );
  for (const query of [
    "status=active",
    "before=5&after=2",
    "limit=10001",
    "before=-1",
    "after=x",
  ]) {
    assert.equal(
      allowedQuery(["runs", "run-1", "logs"], new URLSearchParams(query)),
      false,
    );
  }
  assert.equal(
    allowedQuery(["runs", "x", "logs"], new URLSearchParams("status=active")),
    false,
  );
});

test("backend must be a configured HTTP(S) origin", () => {
  assert.equal(
    backendBase("http://127.0.0.1:8080").origin,
    "http://127.0.0.1:8080",
  );
  assert.equal(
    backendBase("https://scheduler.example").origin,
    "https://scheduler.example",
  );
  for (const url of [
    "file:///etc/passwd",
    "ftp://host",
    "http://user:pass@host",
    "http://host/api",
    "http://host?url=x",
    "http://host#x",
    "not a url",
  ]) {
    assert.throws(() => backendBase(url));
  }
});

test("compares Origin against the actual Host, keeping loopback aliases distinct", () => {
  assert.equal(
    sameOrigin("http://127.0.0.1:3000", "127.0.0.1:3000", "http:"),
    true,
  );
  assert.equal(
    sameOrigin("http://localhost:3000", "localhost:3000", "http:"),
    true,
  );
  assert.equal(sameOrigin("http://[::1]:3000", "[::1]:3000", "http:"), true);
  assert.equal(
    sameOrigin("https://admin.example", "admin.example", "https:"),
    true,
  );
  for (const [origin, host, protocol] of [
    [null, "localhost:3000", "http:"],
    ["http://localhost:3000", "127.0.0.1:3000", "http:"],
    ["http://evil.test", "localhost:3000", "http:"],
    ["https://localhost:3000", "localhost:3000", "http:"],
    ["http://localhost:3001", "localhost:3000", "http:"],
    ["http://localhost:3000/", "localhost:3000", "http:"],
    ["http://evil.test", "user@evil.test", "http:"],
  ]) {
    assert.equal(sameOrigin(origin, host, protocol!), false);
  }
});

test("unauthenticated mode only permits loopback hosts, not DNS rebinding names", () => {
  for (const host of [
    "localhost",
    "localhost:3000",
    "127.0.0.1:3000",
    "[::1]:3000",
  ])
    assert.equal(loopbackHost(host), true);
  for (const host of [
    null,
    "evil.test",
    "127.0.0.1.evil.test",
    "user@localhost",
    "localhost/path",
    "localhost,evil.test",
    "192.168.1.5:3000",
  ])
    assert.equal(loopbackHost(host), false);
});

test("redacts server secrets in nested JSON and keys without changing DTO values", () => {
  assert.deepEqual(
    redact(
      {
        secret: ["before api-token after", { "api-token": "password" }],
        count: 3,
        at: null,
      },
      ["api-token", "password", ""],
    ),
    {
      secret: ["before [redacted] after", { "[redacted]": "[redacted]" }],
      count: 3,
      at: null,
    },
  );
});
