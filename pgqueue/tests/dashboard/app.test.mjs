import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers";

import {
  appendCursor,
  compact,
  cursorView,
  dashboardHome,
  dashboardUrl,
  duration,
  entryRequestKey,
  escapeHtml,
  exceedsJobNameLimit,
  isSuggestionResponseCurrent,
  pageOf,
  parseRoute,
  resetCursor,
} from "../../dashboard/app.mjs";

test("cursor state starts with stable defaults", () => {
  assert.deepEqual(cursorView(), {
    cursor: null,
    history: [],
    start: 1,
    nextCursor: null,
    pageCount: 0,
    limit: 25,
  });
});

test("HTML escaping covers text and attribute metacharacters", () => {
  assert.equal(escapeHtml(`<>&"'`), "&lt;&gt;&amp;&quot;&#39;");
  assert.equal(escapeHtml(42), "42");
});

test("compact numbers switch units without displaying 1000K", () => {
  assert.equal(compact(null), "–");
  assert.equal(compact(9_999), "9999");
  assert.equal(compact(10_000), "10.0K");
  assert.equal(compact(999_949), "999.9K");
  assert.equal(compact(999_950), "1.0M");
});

test("durations use the two largest useful units", () => {
  assert.equal(duration(undefined), "–");
  assert.equal(duration(59_999), "59s");
  assert.equal(duration(61_000), "1m 1s");
  assert.equal(duration(3_661_000), "1h 1m");
  assert.equal(duration(90_000_000), "1d 1h");
});

test("dashboard URLs preserve the mount root", () => {
  assert.equal(dashboardHome(""), "/");
  assert.equal(dashboardHome("/admin"), "/admin");
  assert.equal(dashboardUrl("/admin", "/"), "/admin");
  assert.equal(dashboardUrl("/admin", "/queues/main"), "/admin/queues/main");
});

test("array pagination clamps stale offsets after filtering", () => {
  const view = { offset: 20, limit: 10 };
  assert.deepEqual(pageOf([1, 2, 3], view), [1, 2, 3]);
  assert.equal(view.offset, 0);

  view.offset = 20;
  assert.deepEqual(pageOf(Array.from({ length: 23 }, (_, index) => index), view), [20, 21, 22]);
});

test("cursor helpers reset state and encode a database cursor", () => {
  const view = {
    cursor: { timestamp: "old", id: "old" },
    history: [null],
    start: 26,
    nextCursor: { timestamp: "next", id: "next" },
    pageCount: 25,
  };
  resetCursor(view);
  assert.deepEqual(view, {
    cursor: null,
    history: [],
    start: 1,
    nextCursor: null,
    pageCount: 0,
  });

  const params = new URLSearchParams();
  appendCursor(params, { timestamp: "2026-01-01T00:00:00Z", id: "job-id" }, "cursor_created_at");
  assert.equal(params.get("cursor_created_at"), "2026-01-01T00:00:00Z");
  assert.equal(params.get("cursor_id"), "job-id");
});

test("entry request identity is stable across status insertion order", () => {
  const base = {
    queue: "main",
    name: "mail",
    limit: 25,
    cursor: null,
  };
  const first = entryRequestKey({ ...base, statuses: new Set(["failed", "ready"]) }, "job");
  const second = entryRequestKey({ ...base, statuses: new Set(["ready", "failed"]) }, "job");
  assert.equal(first, second);
  assert.notEqual(first, entryRequestKey({ ...base, statuses: new Set(["ready"]) }, "job"));
});

test("route parsing handles mounts, encoded names, details, and malformed escapes", () => {
  assert.deepEqual(parseRoute("/admin", "/admin"), { view: "home", queue: null, id: null });
  assert.deepEqual(parseRoute("/admin/queues/email%20jobs", "/admin"), {
    view: "queue",
    queue: "email jobs",
    id: null,
  });
  assert.deepEqual(parseRoute("/admin/queues/main/workers/worker-id", "/admin"), {
    view: "worker",
    queue: "main",
    id: "worker-id",
  });
  assert.deepEqual(parseRoute("/queues/main/jobs/job-id", ""), {
    view: "job",
    queue: "main",
    id: "job-id",
  });
  assert.deepEqual(parseRoute("/queues/%ZZ", ""), { view: "queue", queue: "%ZZ", id: null });
  assert.deepEqual(parseRoute("/administrator/queues/main", "/admin"), {
    view: "home",
    queue: null,
    id: null,
  });
});

test("stale suggestion responses cannot replace newer state", () => {
  const view = { suggestionRequest: 4, query: "  email " };
  assert.equal(isSuggestionResponseCurrent(view, 4, "email"), true);
  assert.equal(isSuggestionResponseCurrent(view, 3, "email"), false);
  assert.equal(isSuggestionResponseCurrent(view, 4, "sms"), false);
  assert.equal(isSuggestionResponseCurrent(view, 4), true);
});

test("a job name filter past 255 UTF-8 bytes is recognised as unmatchable", () => {
  assert.equal(exceedsJobNameLimit("x".repeat(255)), false);
  assert.equal(exceedsJobNameLimit("x".repeat(256)), true);
  // Bytes, not characters: 128 two-byte characters are 256 bytes.
  assert.equal(exceedsJobNameLimit("é".repeat(127)), false);
  assert.equal(exceedsJobNameLimit("é".repeat(128)), true);
});

for (const actionKind of ["abort", "retry"]) {
  test(`${actionKind} waits for the destination job to render after Back or Forward`, async (t) => {
    const handlers = new Map();
    const windowHandlers = new Map();
    const requests = [];
    const firstId = "11111111-1111-1111-1111-111111111111";
    const nextId = "22222222-2222-2222-2222-222222222222";
    const location = { pathname: `/queues/main/jobs/${firstId}` };
    const app = {
      innerHTML: "",
      addEventListener: (kind, handler) => handlers.set(kind, handler),
      querySelector: () => null,
      querySelectorAll: () => [],
    };
    const destination = Promise.withResolvers();
    const response = (id) => ({
      ok: true,
      json: async () => ({ job: {
        id, queue: "main", name: "test", kind: "job",
        status: actionKind === "abort" ? "queued" : "failed",
        attempts: 0, max_attempts: 1, priority: 0,
      } }),
    });
    const globals = {
      document: {
        querySelector: () => null,
        getElementById: () => app,
        addEventListener: () => {},
        activeElement: null,
      },
      location,
      window: { location, addEventListener: (kind, handler) => windowHandlers.set(kind, handler) },
      history: { pushState: (_, __, url) => { location.pathname = url; } },
      setInterval: () => 0,
      fetch: async (url, options = {}) => {
        const method = options.method || "GET";
        requests.push({ url, method });
        if (method === "POST") return new Promise(() => {});
        return url.endsWith(firstId) ? response(firstId) : destination.promise;
      },
    };
    for (const [key, value] of Object.entries(globals)) {
      const original = Object.getOwnPropertyDescriptor(globalThis, key);
      Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
      t.after(() => {
        if (original) Object.defineProperty(globalThis, key, original);
        else delete globalThis[key];
      });
    }
    // A fresh module runs the real startup and event handlers against the small DOM fixture above.
    await import(`../../dashboard/app.mjs?navigation-test=${actionKind}`);
    await new Promise(setImmediate);
    assert.ok(app.innerHTML.includes(`Job ${firstId}`));
    const action = {
      dataset: { action: actionKind }, disabled: false, isConnected: true,
      closest: (selector) => selector === "button[data-action]" ? action : null,
    };
    location.pathname = `/queues/main/jobs/${nextId}`;
    windowHandlers.get("popstate")();
    void handlers.get("click")({ target: action, preventDefault() {} });
    assert.ok(app.innerHTML.includes(`Job ${firstId}`), "the old job remains visible during navigation");
    assert.equal(requests.filter(({ method }) => method === "POST").length, 0);

    destination.resolve(response(nextId));
    await new Promise(setImmediate);
    assert.ok(app.innerHTML.includes(`Job ${nextId}`));
    void handlers.get("click")({ target: action, preventDefault() {} });
    assert.deepEqual(requests.filter(({ method }) => method === "POST"), [
      { url: `/api/queues/main/jobs/${nextId}/${actionKind}`, method: "POST" },
    ]);
  });
}
