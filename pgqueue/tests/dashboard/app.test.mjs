import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
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
  matchesNoJobName,
  pageOf,
  parseJsonExactly,
  parseRoute,
  resetCursor,
} from "../../dashboard/app.mjs";

// A fetch stub's successful response, with both of the readers a real `Response` has: the detail views read the body as
// text so integers past 2^53 can stay exact.
const jsonResponse = (body) => ({ ok: true, json: async () => body, text: async () => JSON.stringify(body) });

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
  // The same boundary one unit up, where there used to be no unit to switch to: "1000.0M", then "5000.0M".
  assert.equal(compact(999_949_999), "999.9M");
  assert.equal(compact(999_950_000), "1.0B");
  assert.equal(compact(5_000_000_000), "5.0B");
  assert.equal(compact(999_950_000_000), "1.0T");
});

test("integers past 2^53 survive a detail response digit for digit", () => {
  const body = parseJsonExactly('{"user_id":1234567890123456789,"small":42,"ratio":0.5,"nested":[-9007199254740993]}');
  assert.equal(JSON.stringify(body), '{"user_id":1234567890123456789,"small":42,"ratio":0.5,"nested":[-9007199254740993]}');
  assert.equal(body.small, 42);
  // A stats counter that large is printed as sent, not compared as an object, which threw.
  assert.equal(compact(body.user_id), "1234567890123456789");
  assert.equal(duration(body.user_id), "1234567890123456789");
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

test("a job name filter carrying a NUL is answered locally instead of breaking the page", () => {
  assert.equal(matchesNoJobName("email"), false);
  assert.equal(matchesNoJobName("mail\0"), true);
  assert.equal(matchesNoJobName("x".repeat(256)), true);
});

test("editing a search removes old suggestions before keyboard selection can commit them", async (t) => {
  const handlers = new Map();
  const timers = new Map();
  const requests = [];
  const attributes = new Map();
  let markup = "";
  let options = [];
  let timerId = 0;
  const input = {
    id: "job-name-filter", value: "", focus() {},
    closest: () => null,
    matches: (selector) => selector.includes("#job-name-filter"),
    setAttribute: (name, value) => attributes.set(name, value),
    removeAttribute: (name) => attributes.delete(name),
  };
  const app = {
    get innerHTML() { return markup; },
    set innerHTML(value) {
      markup = value;
      options = [...value.matchAll(/data-job-name="([^"]+)"/g)].map((match, index) => ({
        id: `job-name-suggestion-${index}`, dataset: { jobName: match[1] },
        setAttribute() {}, scrollIntoView() {},
      }));
    },
    addEventListener: (kind, handler) => handlers.set(kind, handler),
    querySelector: (selector) => {
      if (selector === ".job-name-search input[role=combobox]") return input;
      if (selector === ".name-suggestions" && options.length) {
        return { remove() { options = []; }, closest: () => null };
      }
      return null;
    },
    querySelectorAll: (selector) => selector === ".name-suggestions [role=option]" ? options : [],
  };
  const globals = {
    document: {
      // Focus is in the search box being typed into: suggestions open only there.
      querySelector: () => null, getElementById: (id) => (id === input.id ? input : app),
      addEventListener() {}, activeElement: input,
    },
    location: { pathname: "/queues/main" },
    window: { addEventListener() {} },
    setInterval: () => 0,
    setTimeout: (callback) => { timers.set(++timerId, callback); return timerId; },
    clearTimeout: (id) => timers.delete(id),
    fetch: async (url) => {
      requests.push(url);
      const body = url.includes("/job-names?") ? { names: ["email"] }
        : url.includes("/workers?") ? { workers: [] } : { jobs: [] };
      return { ok: true, json: async () => body };
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
  await import("../../dashboard/app.mjs?search-edit-test");
  await new Promise(setImmediate);
  input.value = "em";
  handlers.get("input")({ target: input });
  for (const callback of timers.values()) callback();
  timers.clear();
  await new Promise(setImmediate);
  assert.equal(options.length, 1);

  // The next search is still debouncing, but the old options must already be gone.
  input.value = "sms";
  handlers.get("input")({ target: input });
  handlers.get("keydown")({ target: input, key: "ArrowDown", preventDefault() {} });
  await handlers.get("submit")({
    target: {
      matches: (selector) => [".search-filter", ".job-name-search"].includes(selector),
      dataset: { entryKind: "job" },
    },
    preventDefault() {},
  });
  await new Promise(setImmediate);
  const filtered = requests.filter((url) => url.includes("/jobs?"));
  assert.equal(new URL(filtered.at(-1), "http://localhost").searchParams.get("name"), "sms");
  assert.equal(attributes.get("aria-expanded"), "false");
});

test("repaints and the suggestion keys wait for an IME composition in the search box to end", async (t) => {
  const handlers = new Map();
  const timers = new Map();
  let markup = "";
  let paints = 0;
  let poll;
  let timerId = 0;
  const input = {
    id: "job-name-filter", value: "", focus() {},
    closest: () => null,
    matches: (selector) => selector.includes("#job-name-filter"),
    setAttribute() {}, removeAttribute() {},
  };
  const app = {
    get innerHTML() { return markup; },
    set innerHTML(value) { markup = value; paints += 1; },
    addEventListener: (kind, handler) => handlers.set(kind, handler),
    querySelector: (selector) => selector === ".job-name-search input[role=combobox]" ? input : null,
    querySelectorAll: () => [],
  };
  const globals = {
    document: {
      // Focus is in the search box being typed into: suggestions open only there.
      querySelector: () => null, getElementById: (id) => (id === input.id ? input : app),
      addEventListener() {}, activeElement: input, visibilityState: "visible",
    },
    location: { pathname: "/queues/main" },
    window: { addEventListener() {} },
    setInterval: (callback) => { poll = callback; return 0; },
    setTimeout: (callback) => { timers.set(++timerId, callback); return timerId; },
    clearTimeout: (id) => timers.delete(id),
    fetch: async (url) => {
      const body = url.includes("/job-names?") ? { names: ["東京"] }
        : url.includes("/workers?") ? { workers: [] } : { jobs: [] };
      return { ok: true, json: async () => body };
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
  await import("../../dashboard/app.mjs?composition-test");
  await new Promise(setImmediate);
  const painted = paints;

  // A Japanese reading waits in the input for conversion; replacing the input ends the composition.
  handlers.get("compositionstart")?.({ target: input });
  input.value = "とうきょう";
  handlers.get("input")({ target: input, isComposing: true });
  for (const callback of timers.values()) callback();
  timers.clear();
  await new Promise(setImmediate);
  poll();
  await new Promise(setImmediate);
  assert.equal(paints, painted, "a repaint replaced the input mid-composition");

  // The suggestions have landed, but mid-composition the arrows pick the IME's candidates.
  let prevented = false;
  handlers.get("keydown")({
    target: input, key: "ArrowDown", isComposing: true, preventDefault() { prevented = true; },
  });
  assert.equal(prevented, false, "the arrow key was taken from the IME");

  handlers.get("compositionend")?.({ target: input });
  await new Promise(setImmediate);
  assert.ok(paints > painted, "the deferred repaint never ran");
  assert.match(markup, /value="とうきょう"/);
  assert.match(markup, /data-job-name="東京"/);
});

test("retry is offered only for a terminal job that has not been retried already", async (t) => {
  const handlers = new Map();
  const id = "33333333-3333-3333-3333-333333333333";
  const app = {
    innerHTML: "",
    addEventListener: (kind, handler) => handlers.set(kind, handler),
    querySelector: () => null,
    querySelectorAll: () => [],
  };
  const globals = {
    document: {
      querySelector: () => null, getElementById: () => app,
      addEventListener() {}, activeElement: null,
    },
    location: { pathname: `/queues/main/jobs/${id}` },
    window: { addEventListener() {} },
    setInterval: () => 0,
    fetch: async () => jsonResponse({ job: {
      id, queue: "main", name: "test", kind: "job", status: "failed",
      attempts: 1, max_attempts: 1, priority: 0, retried_at: "2026-01-01T00:00:00Z",
    } }),
  };
  for (const [key, value] of Object.entries(globals)) {
    const original = Object.getOwnPropertyDescriptor(globalThis, key);
    Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
    t.after(() => {
      if (original) Object.defineProperty(globalThis, key, original);
      else delete globalThis[key];
    });
  }
  await import("../../dashboard/app.mjs?retried-test");
  await new Promise(setImmediate);
  // A row carries at most one retry: the server refuses a second one, so the button must not offer it.
  assert.match(app.innerHTML, /id="action-retry" data-action="retry" disabled/);
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
    const response = (id) => jsonResponse({ job: {
      id, queue: "main", name: "test", kind: "job",
      status: actionKind === "abort" ? "queued" : "failed",
      attempts: 0, max_attempts: 1, priority: 0,
    } });
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

const settle = async (turns = 5) => {
  for (let turn = 0; turn < turns; turn += 1) await new Promise(setImmediate);
};

const failedResponse = (status, error) => ({ ok: false, status, statusText: "", json: async () => ({ error }) });

// A document selection over text in #app, collapsed until a test drags it open; dropping it collapses it again.
const textSelection = () => ({
  isCollapsed: true, anchorNode: {}, drops: 0,
  removeAllRanges() { this.drops += 1; this.isCollapsed = true; },
});

// A stand-in for one element of a painted page that the module looks up again, by its `id` or by one of the few
// classes it queries. Its attributes and `data-*` come from its start tag, and `ancestors` answers `closest()`.
// Focusing it moves the document's focus there and tells the `focusin` listener, as a browser does.
const fakeElement = (page, attributes, ancestors = {}) => {
  const attributeValues = new Map(Object.entries(attributes));
  const classes = new Set((attributes.class ?? "").split(/\s+/).filter(Boolean));
  const element = {
    id: attributes.id ?? "", value: attributes.value ?? "", textContent: "", removed: false, focusCalls: [],
    dataset: Object.fromEntries(Object.entries(attributes)
      .filter(([name]) => name.startsWith("data-"))
      .map(([name, value]) => [name.slice(5).replace(/-(\w)/g, (_, letter) => letter.toUpperCase()), value])),
    hasAttribute: (name) => attributeValues.has(name),
    getAttribute: (name) => attributeValues.get(name) ?? null,
    setAttribute: (name, value) => { attributeValues.set(name, String(value)); },
    removeAttribute: (name) => { attributeValues.delete(name); },
    classList: {
      contains: (name) => classes.has(name),
      remove: (name) => { classes.delete(name); },
      toggle: (name, force = !classes.has(name)) => {
        if (force) classes.add(name);
        else classes.delete(name);
        return force;
      },
    },
    matches: (selector) => selector.split(",").some((part) => part.trim() === `#${element.id}`),
    closest: (selector) => ancestors[selector] ?? null,
    focus(options) {
      element.focusCalls.push(options);
      page.document.activeElement = element;
      page.documentHandlers.get("focusin")?.({ target: element });
    },
    remove() { element.removed = true; },
    scrollIntoView() {},
  };
  return element;
};

// The stand-ins for freshly painted markup: one for each element with an `id`, plus the account menu, the account
// message and the job name search, which have none. Whatever sits inside that search finds it, and the page's scroll
// box, with `closest()`; both report the box `page.layout` gives them.
const paintElements = (page, markup) => {
  page.elements = new Map();
  page.menu = null;
  page.accountMessage = null;
  page.search = null;
  const searchStart = markup.indexOf('<form class="search-filter job-name-search"');
  const searchEnd = searchStart < 0 ? -1 : markup.indexOf("</form>", searchStart);
  for (const { 1: source, index } of markup.matchAll(/<[a-z]+\s([^>]*)>/g)) {
    const attributes = Object.fromEntries(
      [...source.matchAll(/([\w-]+)(?:="([^"]*)")?/g)].map(([, name, value = ""]) => [name, value]),
    );
    const classes = (attributes.class ?? "").split(/\s+/);
    if (classes.includes("job-name-search")) {
      page.search = fakeElement(page, attributes);
      page.search.getBoundingClientRect = () => page.layout.search;
    } else if (classes.includes("account-menu")) {
      page.menu = fakeElement(page, attributes);
    } else if (classes.includes("account-message")) {
      page.accountMessage = fakeElement(page, attributes);
      page.accountMessage.replaceWith = (node) => { page.accountMessage = node; };
    }
    if (attributes.id === undefined) continue;
    const inSearch = index > searchStart && index < searchEnd;
    page.elements.set(attributes.id, fakeElement(page, attributes, inSearch
      ? { ".job-name-search": page.search, ".page-content": page.scroller("page") }
      : {}));
  }
  const list = page.elements.get("job-name-suggestions");
  if (list) Object.defineProperty(list, "offsetHeight", { get: () => page.layout.list });
};

// Boots a fresh copy of the real module against a small DOM, for the tests below that follow a page across repaints.
// Every paint gives `#app` a fresh element for each `data-scroll-key` and refresh notice in the markup, the way a real
// repaint replaces them, and the stand-ins `paintElements` makes; `page.poll` is the 5s refresh, and `page.runTimers`
// fires whatever `setTimeout` is waiting on, such as the search box's debounce. Back and Forward set `page.location`,
// then run the `popstate` handler, as a browser does.
const bootDashboard = async (t, { tag, path, fetch, root = "", selection = null, auth = false }) => {
  const location = { pathname: path };
  let timerId = 0;
  const page = {
    markup: "", paints: 0, scrollers: [], notice: null, poll: null, location,
    handlers: new Map(), documentHandlers: new Map(), windowHandlers: new Map(), timers: new Map(),
    elements: new Map(), menu: null, accountMessage: null, search: null,
    // Where the page's scroll box, the job name search and its suggestion list sit, in pixels; a tall screen at first.
    layout: { port: { top: 0, bottom: 1000 }, search: { top: 100, bottom: 136 }, list: 200 },
    scroller: (key) => page.scrollers.find((element) => element.dataset.scrollKey === key),
    element: (id) => page.elements.get(id),
    list: () => {
      const list = page.element("job-name-suggestions");
      return list && !list.removed ? list : undefined;
    },
    options: () => (page.list()
      ? [...page.elements.values()].filter((element) => element.getAttribute("role") === "option")
      : []),
    runTimers: () => {
      const due = [...page.timers.values()];
      page.timers.clear();
      for (const callback of due) callback();
    },
  };
  const app = {
    get innerHTML() { return page.markup; },
    set innerHTML(value) {
      page.markup = value;
      page.paints += 1;
      page.scrollers = [...value.matchAll(/data-scroll-key="([^"]*)"/g)].map(([, scrollKey]) => ({
        dataset: { scrollKey }, scrollLeft: 0, scrollTop: 0, focusCalls: [],
        focus(options) { this.focusCalls.push(options); },
        ...(scrollKey === "page" ? { getBoundingClientRect: () => page.layout.port } : {}),
      }));
      page.notice = value.includes('class="refresh-error"') ? { hidden: true, textContent: "", title: "" } : null;
      paintElements(page, value);
    },
    addEventListener: (kind, handler) => page.handlers.set(kind, handler),
    contains: (node) => page.scrollers.includes(node) || (node != null && node === selection?.anchorNode),
    querySelector: (selector) => {
      if (selector === ".refresh-error") return page.notice;
      if (selector === ".account-menu") return page.menu;
      if (selector === ".account-menu[open]") return page.menu?.hasAttribute("open") ? page.menu : null;
      if (selector === ".account-message") return page.accountMessage;
      if (selector === ".job-name-search input[role=combobox]") {
        return page.element("job-name-filter") ?? page.element("cron-name-filter") ?? null;
      }
      if (selector === ".name-suggestions") return page.list() ?? null;
      if (selector === '.name-suggestions [role=option][aria-selected="true"]') {
        return page.options().find((option) => option.getAttribute("aria-selected") === "true") ?? null;
      }
      const scrollKey = /^\[data-scroll-key="([^"]*)"\]$/.exec(selector)?.[1];
      return scrollKey === undefined ? null : page.scroller(scrollKey) ?? null;
    },
    querySelectorAll: (selector) => {
      if (selector === "[data-scroll-key]") return page.scrollers;
      if (selector === ".name-suggestions [role=option]") return page.options();
      return [];
    },
  };
  page.document = {
    querySelector: (selector) => {
      if (selector === 'meta[name="pgqueue-root"]') return { content: root };
      if (selector === 'meta[name="pgqueue-auth-enabled"]') return { content: String(auth) };
      return null;
    },
    getElementById: (id) => (id === "app" ? app : page.element(id) ?? null),
    addEventListener: (kind, handler) => page.documentHandlers.set(kind, handler),
    activeElement: null,
    visibilityState: "visible",
    getSelection: () => selection,
  };
  const globals = {
    document: page.document,
    location,
    window: { location, addEventListener: (kind, handler) => page.windowHandlers.set(kind, handler) },
    history: { pushState: (_, __, url) => { location.pathname = url; } },
    setInterval: (callback) => { page.poll = callback; return 0; },
    setTimeout: (callback) => {
      page.timers.set(++timerId, callback);
      return timerId;
    },
    clearTimeout: (id) => { page.timers.delete(id); },
    fetch,
  };
  for (const [key, value] of Object.entries(globals)) {
    const original = Object.getOwnPropertyDescriptor(globalThis, key);
    Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
    t.after(() => {
      if (original) Object.defineProperty(globalThis, key, original);
      else delete globalThis[key];
    });
  }
  await import(`../../dashboard/app.mjs?${tag}`);
  await settle();
  return page;
};

// A click on a control the handler finds by `selector`, the way `closest()` finds it from the clicked node.
const clickOn = (page, selector, dataset) => {
  const node = { dataset, disabled: false, isConnected: true };
  node.closest = (wanted) => (wanted === selector ? node : null);
  return page.handlers.get("click")({ target: node, preventDefault() {} });
};

const queueJobs = (count, offset = 0) => Array.from({ length: count }, (_, index) => ({
  id: `00000000-0000-7000-8000-${String(offset + index).padStart(12, "0")}`, name: "email", status: "failed",
  attempts: 1, max_attempts: 1, scheduled_at: null, completed_at: null,
}));

const lastListing = (requests) => new URL(requests.findLast((url) => url.includes("/jobs?")), "http://localhost");

// Every `@media` block of a stylesheet: its condition, and the declarations each selector inside it is given.
const mediaBlocks = (stylesheet) => {
  const css = stylesheet.replace(/\/\*[\s\S]*?\*\//g, "");
  return [...css.matchAll(/@media([^{]*)\{/g)].map((match) => {
    const start = match.index + match[0].length;
    let end = start;
    for (let depth = 1; depth > 0 && end < css.length; end += 1) {
      if (css[end] === "{") depth += 1;
      if (css[end] === "}") depth -= 1;
    }
    const rules = new Map();
    for (const [, selectors, body] of css.slice(start, end - 1).matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
      const declarations = body.split(";").filter((part) => part.includes(":")).map((part) => {
        const colon = part.indexOf(":");
        return [part.slice(0, colon).trim(), part.slice(colon + 1).trim()];
      });
      for (const selector of selectors.split(",").map((part) => part.trim())) {
        rules.set(selector, { ...rules.get(selector), ...Object.fromEntries(declarations) });
      }
    }
    return { condition: match[1], rules };
  });
};

test("the queue page keeps its own scroll position across a repaint", async (t) => {
  let workers = [];
  const page = await bootDashboard(t, {
    tag: "page-scroll-test",
    path: "/queues/main",
    fetch: async (url) => jsonResponse(url.includes("/workers?") ? { workers } : { jobs: queueJobs(3) }),
  });
  // On a short or narrow screen the page scrolls as a whole (see app.css), and a poll rebuilds it.
  assert.match(page.markup, /class="page-content queue-page" data-scroll-key="page"/);
  page.scroller("page").scrollTop = 282;
  workers = [{ id: "worker-1", stats: { complete: 1 } }];
  page.poll();
  await settle();
  assert.equal(page.paints, 2, "the poll did not repaint");
  assert.equal(page.scroller("page").scrollTop, 282, "the repaint threw the page back to its top");
});

test("short and narrow screens scroll the queue page instead of squeezing the jobs table to nothing", async () => {
  const blocks = mediaBlocks(await readFile(new URL("../../dashboard/app.css", import.meta.url), "utf8"));
  const fix = blocks.find(({ condition, rules }) => {
    if (!/max-height/.test(condition) || !/max-width/.test(condition)) return false;
    const content = rules.get(".page-content") ?? {};
    const scrolls = ["auto", "scroll"].includes(content["overflow-y"] ?? content.overflow);
    // `max-content`, not `auto`: the sections carry `min-height: 0`, so an `auto` row squeezes the workers table.
    const rows = /^max-content\s+minmax\(\s*([^,]+?)\s*,\s*1fr\s*\)$/
      .exec(rules.get(".queue-page")?.["grid-template-rows"] ?? "");
    return scrolls && rows != null && Number.parseFloat(rows[1]) > 0;
  });
  assert.ok(fix, "no media block lets a short or narrow queue page scroll with a floor under the jobs table");
});

test("a failed refresh leaves the page on screen and says so until one succeeds", async (t) => {
  const id = "44444444-4444-4444-4444-444444444444";
  for (const [kind, failure] of [
    ["server error", () => failedResponse(500, "internal server error")],
    ["timeout", () => failedResponse(504, "dashboard request timed out")],
    ["saturation", () => failedResponse(429, "too many requests")],
    ["network failure", () => Promise.reject(new TypeError("Failed to fetch"))],
  ]) {
    await t.test(kind, async (t) => {
      let fail = false;
      const page = await bootDashboard(t, {
        tag: `refresh-failure-test=${encodeURIComponent(kind)}`,
        path: `/queues/main/jobs/${id}`,
        fetch: async () => fail ? failure() : jsonResponse({ job: {
          id, queue: "main", name: "test", kind: "job", status: "failed", attempts: 1, max_attempts: 1, priority: 0,
        } }),
      });
      const painted = page.markup;
      assert.ok(painted.includes(`Job ${id}`));
      assert.equal(page.notice?.hidden, true, "the page has no refresh notice to fill");

      // Replacing #app would take the job page down with it: an unread action message, a search box mid-word.
      fail = true;
      page.poll();
      await settle();
      assert.equal(page.paints, 1, "a failed refresh repainted the page");
      assert.equal(page.markup, painted);
      assert.equal(page.notice.hidden, false);
      assert.match(page.notice.textContent, /^Refresh failed: /);

      // The page is unchanged, so the recovery repaints nothing: the notice has to clear itself.
      fail = false;
      page.poll();
      await settle();
      assert.equal(page.paints, 1);
      assert.equal(page.notice.hidden, true, "the notice outlived the refresh that recovered");
      assert.equal(page.notice.textContent, "");
    });
  }
});

test("a job that is gone, or a page that never loaded, still shows the error page", async (t) => {
  const id = "55555555-5555-5555-5555-555555555555";
  let answer = () => jsonResponse({ job: {
    id, queue: "main", name: "test", kind: "job", status: "failed", attempts: 1, max_attempts: 1, priority: 0,
  } });
  const page = await bootDashboard(t, {
    tag: "gone-page-test", path: `/queues/main/jobs/${id}`, fetch: async () => answer(),
  });
  assert.ok(page.markup.includes(`Job ${id}`));
  // Purged while on screen: a definite answer about this page, not a refresh to retry.
  answer = () => failedResponse(404, "job not found");
  page.poll();
  await settle();
  assert.match(page.markup, /<article class="error-banner">job not found<\/article>/);

  const first = await bootDashboard(t, {
    tag: "first-load-failure-test",
    path: "/queues/main",
    fetch: async () => failedResponse(500, "internal server error"),
  });
  assert.match(first.markup, /<article class="error-banner">internal server error<\/article>/);
});

test("an error page keeps the account menu, message and focus, and the same answer does not rebuild it", async (t) => {
  const id = "88888888-8888-8888-8888-888888888888";
  let answer = () => jsonResponse({ job: {
    id, queue: "main", name: "test", kind: "job", status: "failed", attempts: 1, max_attempts: 1, priority: 0,
  } });
  const page = await bootDashboard(t, {
    tag: "steady-error-page-test", path: `/queues/main/jobs/${id}`, auth: true, fetch: async () => answer(),
  });
  // The operator has the account menu open, the home breadcrumb focused, and a password they have just changed.
  page.menu.setAttribute("open", "");
  page.accountMessage.textContent = "Password changed";
  page.element("breadcrumb-%2F").focus();

  // Purged while on screen: every poll from now on gets the same 404, as does a stopped worker's page.
  answer = () => failedResponse(404, "job not found");
  for (let poll = 1; poll <= 3; poll += 1) {
    page.poll();
    await settle();
    assert.match(page.markup, /<article class="error-banner">job not found<\/article>/);
    assert.equal(page.paints, 2, `poll ${poll} rebuilt an error page that had not changed`);
    assert.equal(page.menu.hasAttribute("open"), true, `the account menu closed at poll ${poll}`);
    assert.equal(page.accountMessage.textContent, "Password changed", `poll ${poll} wiped the account message`);
    assert.equal(page.document.activeElement, page.element("breadcrumb-%2F"), `focus was lost at poll ${poll}`);
  }
});

test("an error page is repainted only when the answer changes", async (t) => {
  let answer = () => failedResponse(500, "internal server error");
  const page = await bootDashboard(t, {
    tag: "error-page-answer-test", path: "/queues/main", fetch: async (url) => answer(url),
  });
  assert.match(page.markup, /<article class="error-banner">internal server error<\/article>/);
  page.poll();
  await settle();
  assert.equal(page.paints, 1, "the same failure rebuilt the error page");

  answer = () => failedResponse(404, "queue not found");
  page.poll();
  await settle();
  assert.equal(page.paints, 2, "a different answer left the old error on screen");
  assert.match(page.markup, /<article class="error-banner">queue not found<\/article>/);

  // The page loads at last, and replaces the error.
  answer = (url) => jsonResponse(url.includes("/workers?") ? { workers: [] } : { jobs: queueJobs(1) });
  page.poll();
  await settle();
  assert.equal(page.paints, 3);
  assert.doesNotMatch(page.markup, /error-banner/);
  assert.match(page.markup, /class="page-content queue-page"/);
});

test("leaving a page cancels its stalled read instead of waiting it out, and reports no error for it", async (t) => {
  const firstId = "11111111-1111-1111-1111-111111111111";
  const secondId = "22222222-2222-2222-2222-222222222222";
  const thirdId = "33333333-3333-3333-3333-333333333333";
  const response = (id) => jsonResponse({ job: {
    id, queue: "main", name: "test", kind: "job", status: "failed", attempts: 1, max_attempts: 1, priority: 0,
  } });
  const requests = [];
  const stalled = new Set();
  const thirdRead = Promise.withResolvers();
  const page = await bootDashboard(t, {
    tag: "stalled-read-test",
    path: `/queues/main/jobs/${firstId}`,
    fetch: async (url, { signal } = {}) => {
      requests.push(url);
      const id = url.split("/").at(-1);
      if (id === thirdId) return thirdRead.promise;
      if (!stalled.has(id)) return response(id);
      // A read the server never answers. As with a real fetch, giving up on it rejects it with the signal's reason, an
      // AbortError.
      return new Promise((_, reject) => signal?.addEventListener("abort", () => reject(signal.reason)));
    },
  });
  const painted = page.markup;
  assert.ok(painted.includes(`Job ${firstId}`));

  // The 5s refresh of the first job stalls on the server, holding the one render slot.
  stalled.add(firstId);
  page.poll();
  await settle();
  // Back to the second job, whose read stalls as well. The cancelled refresh rejects without a status, just like one
  // the network dropped (see `retryableFailure`), but nothing failed: the header must not say it did.
  stalled.add(secondId);
  page.location.pathname = `/queues/main/jobs/${secondId}`;
  page.windowHandlers.get("popstate")();
  await settle();
  assert.ok(requests.some((url) => url.endsWith(secondId)), "the stalled refresh still held the render slot");
  assert.equal(page.notice.hidden, true, "a cancelled refresh was reported as a failed one");

  // Forward to the third job before the second has answered. The navigation this cancels did not fail either: the
  // first job stays on screen until the third is ready, where a failed navigation would have put the error page.
  page.location.pathname = `/queues/main/jobs/${thirdId}`;
  page.windowHandlers.get("popstate")();
  await settle();
  assert.ok(requests.some((url) => url.endsWith(thirdId)), "the stalled navigation still held the render slot");
  assert.equal(page.markup, painted, "a cancelled navigation was shown as an error");

  thirdRead.resolve(response(thirdId));
  await settle();
  assert.ok(page.markup.includes(`Job ${thirdId}`));
  assert.equal(page.paints, 2, "a cancelled read painted something on the way");
});

for (const { status, abortBody, returnToFirst } of [
  { status: 404, abortBody: true, returnToFirst: false },
  { status: 500, abortBody: true, returnToFirst: false },
  { status: 404, abortBody: true, returnToFirst: true },
  { status: 404, abortBody: false, returnToFirst: false },
  { status: 500, abortBody: false, returnToFirst: false },
]) {
  const variant = `${status}-${abortBody ? "aborted" : "late"}-${returnToFirst ? "return" : "leave"}`;
  test(`navigation ignores an obsolete error response body (${variant})`, async (t) => {
    const firstId = "11111111-1111-1111-1111-111111111111";
    const secondId = "22222222-2222-2222-2222-222222222222";
    const response = (id) => jsonResponse({ job: {
      id, queue: "main", name: "test", kind: "job", status: "failed", attempts: 1, max_attempts: 1, priority: 0,
    } });
    const errorBody = Promise.withResolvers();
    const destinationRead = Promise.withResolvers();
    const requests = [];
    let refresh = false;
    let serveError = true;
    const page = await bootDashboard(t, {
      tag: `obsolete-error-body-${variant}`,
      path: `/queues/main/jobs/${firstId}`,
      fetch: async (url, { signal } = {}) => {
        requests.push(url);
        if (!refresh) return response(firstId);
        if (!serveError) return destinationRead.promise;
        serveError = false;
        // Headers have arrived, but reading the failed response's body is still pending when navigation starts.
        return {
          ok: false, status, statusText: "",
          json: () => {
            if (abortBody) signal.addEventListener("abort", () => errorBody.reject(signal.reason));
            return errorBody.promise;
          },
        };
      },
    });
    const painted = page.markup;
    refresh = true;
    page.poll();
    await settle();

    page.location.pathname = `/queues/main/jobs/${secondId}`;
    page.windowHandlers.get("popstate")();
    if (returnToFirst) {
      // Returning before the body rejection settles makes a pathname check alone insufficient.
      page.location.pathname = `/queues/main/jobs/${firstId}`;
      page.windowHandlers.get("popstate")();
    }
    if (!abortBody) errorBody.reject(new Error("response body failed after navigation"));
    await settle();

    const destinationId = returnToFirst ? firstId : secondId;
    assert.equal(page.markup, painted, "an obsolete response replaced the page");
    assert.equal(page.notice.hidden, true, "an obsolete response reported a refresh failure");
    assert.equal(page.paints, 1, "an obsolete response repainted the page");
    assert.equal(requests.length, 3, "the destination did not start a fresh read");
    assert.ok(requests.at(-1).endsWith(destinationId), "the obsolete response still held the render slot");
    destinationRead.resolve(response(destinationId));
    await settle();
    assert.ok(page.markup.includes(`Job ${destinationId}`));
    assert.equal(page.notice.hidden, true);
  });
}

for (const scrollKey of ["job-payload", "job-details"]) {
  test(`keyboard focus on the ${scrollKey} scroll box survives a repaint`, async (t) => {
    const id = "66666666-6666-6666-6666-666666666666";
    let updates = 0;
    const page = await bootDashboard(t, {
      tag: `scroller-focus-test=${scrollKey}`,
      path: `/queues/main/jobs/${id}`,
      fetch: async () => {
        updates += 1;
        return jsonResponse({ job: {
          id, queue: "main", name: "test", kind: "job", status: "running", attempts: 1, max_attempts: 1, priority: 0,
          payload: { key: "value" }, updated_at: `2026-01-01T00:00:0${updates}Z`,
        } });
      },
    });
    // A box that overflows is a tab stop in Chrome and Firefox, and has no id to be found again by.
    page.document.activeElement = page.scroller(scrollKey);
    page.poll();
    await settle();
    assert.equal(page.paints, 2, "the job's change did not repaint");
    assert.deepEqual(page.scroller(scrollKey).focusCalls, [{ preventScroll: true }]);

    // Focus outside every scroll box restores none of them.
    page.document.activeElement = { id: "" };
    page.poll();
    await settle();
    assert.equal(page.paints, 3);
    assert.deepEqual(page.scrollers.flatMap((element) => element.focusCalls), []);
  });
}

for (const [version, suffix] of [["testfp", "?v=testfp"], [null, ""]]) {
  test(`the breadcrumb logo carries the fingerprint the module was loaded with (${version ?? "none"})`, async (t) => {
    const page = await bootDashboard(t, {
      tag: version ? `v=${version}&case=breadcrumb-logo` : "case=breadcrumb-logo-unversioned",
      root: "/admin",
      path: "/admin",
      fetch: async () => jsonResponse({ queues: [] }),
    });
    // `/static/` files are cached for an hour, and only a fingerprinted URL is fetched afresh after an upgrade.
    const assets = [...page.markup.matchAll(/(?:src|href)="([^"]*\/static\/[^"]*)"/g)].map(([, href]) => href);
    assert.deepEqual(assets, [`/admin/static/favicon.svg${suffix}`]);
  });
}

test("a same-page action is not held back by a text selection that a poll waits for", async (t) => {
  const selection = textSelection();
  const requests = [];
  let workers = [];
  const page = await bootDashboard(t, {
    tag: "selection-filter-test",
    path: "/queues/main",
    selection,
    fetch: async (url) => {
      requests.push(url);
      return jsonResponse(url.includes("/workers?") ? { workers } : { jobs: queueJobs(3) });
    },
  });
  assert.equal(page.paints, 1);
  // Nothing selected, nothing dropped: a caret, such as the one in a search box, reads as a collapsed selection.
  await clickOn(page, "button[data-kind]", { kind: "job" });
  await settle();
  assert.equal(selection.drops, 0);

  // The operator is copying something off the page: a poll waits rather than replacing the text under the selection.
  selection.isCollapsed = false;
  workers = [{ id: "worker-1", stats: { complete: 1 } }];
  page.poll();
  await settle();
  assert.equal(page.paints, 1, "a poll replaced the selected text");

  // A filter is a request for a new page, which no selection should hold back.
  await clickOn(page, "button[data-status]", { entryKind: "job", status: "failed" });
  await settle();
  assert.equal(lastListing(requests).searchParams.get("status"), "failed");
  assert.match(
    page.markup,
    /id="status-filter-job-failed" data-entry-kind="job" data-status="failed" aria-pressed="true"/,
  );
  assert.equal(selection.drops, 1);
});

test("a repaint deferred for a selection lands as soon as the selection goes", async (t) => {
  const selection = textSelection();
  let workers = [];
  const page = await bootDashboard(t, {
    tag: "selection-flush-test",
    path: "/queues/main",
    selection,
    fetch: async (url) => jsonResponse(url.includes("/workers?") ? { workers } : { jobs: queueJobs(3) }),
  });
  selection.isCollapsed = false;
  workers = [{ id: "worker-1", stats: { complete: 1 } }];
  page.poll();
  await settle();
  assert.equal(page.paints, 1);
  selection.isCollapsed = true;
  page.documentHandlers.get("selectionchange")?.();
  await settle();
  assert.equal(page.paints, 2, "the deferred repaint waited for the next poll");
  assert.match(page.markup, /id="row-worker-worker-1"/);
});

test("paging is not held back by a text selection", async (t) => {
  const selection = textSelection();
  const requests = [];
  const page = await bootDashboard(t, {
    tag: "selection-pager-test",
    path: "/queues/main",
    selection,
    fetch: async (url) => {
      requests.push(url);
      if (url.includes("/workers?")) return jsonResponse({ workers: [] });
      const second = new URL(url, "http://localhost").searchParams.has("cursor_id");
      const jobs = queueJobs(25, second ? 25 : 0);
      return jsonResponse({
        jobs, next_cursor: second ? null : { enqueued_at: "2026-01-01T00:00:00Z", id: jobs.at(-1).id },
      });
    },
  });
  assert.match(page.markup, /Showing 1-25/);
  selection.isCollapsed = false;
  await clickOn(page, "button[data-page]", { pager: "jobs", page: "1" });
  await settle();
  assert.ok(lastListing(requests).searchParams.has("cursor_id"), "Next asked for nothing");
  assert.match(page.markup, /Showing 26-50/);
});

test("an applied Abort repaints the job even with text selected on the page", async (t) => {
  const id = "77777777-7777-7777-7777-777777777777";
  const selection = textSelection();
  let status = "queued";
  const page = await bootDashboard(t, {
    tag: "selection-abort-test",
    path: `/queues/main/jobs/${id}`,
    selection,
    fetch: async (url, options = {}) => {
      if (options.method === "POST") {
        status = "aborted";
        return jsonResponse({ aborted: true });
      }
      return jsonResponse({ job: {
        id, queue: "main", name: "test", kind: "job", status, attempts: 0, max_attempts: 1, priority: 0,
        payload: { to: "someone@example.com" },
      } });
    },
  });
  assert.match(page.markup, /<span class="status queued">/);
  // The copy this deferral exists for: part of the payload, selected, then Abort.
  selection.isCollapsed = false;
  await clickOn(page, "button[data-action]", { action: "abort" });
  await settle();
  assert.match(page.markup, /<span class="status aborted">/, "the aborted job still showed as queued");
  assert.match(page.markup, /id="action-retry" data-action="retry" >Retry/);
});

test("a drag that selects text in a row keeps the selection instead of opening the row's page", async (t) => {
  const jobs = queueJobs(2);
  const selection = textSelection();
  const page = await bootDashboard(t, {
    tag: "row-selection-test",
    path: "/queues/main",
    selection,
    fetch: async (url) => {
      if (url.includes("/workers?")) return jsonResponse({ workers: [] });
      if (url.includes("/jobs?")) return jsonResponse({ jobs });
      const job = jobs.find(({ id }) => url.endsWith(id));
      return jsonResponse({ job: { ...job, queue: "main", kind: "job", priority: 0 } });
    },
  });
  const row = (job) => page.element(`row-entry-${job.id}`);
  // The click a browser fires when the button goes down and comes up again inside one row.
  const clickRow = (job) => page.handlers.get("click")({
    target: { closest: (selector) => (selector === "tr[data-row-nav]" ? row(job) : null) },
    button: 0,
    preventDefault() {},
  });

  // A drag across the first job's name ends in a click on its row, with that name selected.
  selection.isCollapsed = false;
  selection.containsNode = (node, partly) => partly === true && node === row(jobs[0]);
  await clickRow(jobs[0]);
  await settle();
  assert.equal(page.location.pathname, "/queues/main", "the drag opened the row's page");
  assert.equal(selection.drops, 0, "the selection was dropped");

  // Text selected outside a row holds back no click on it.
  await clickRow(jobs[1]);
  await settle();
  assert.equal(page.location.pathname, `/queues/main/jobs/${jobs[1].id}`);

  // Nor does the collapsed caret a plain click leaves.
  page.location.pathname = "/queues/main";
  page.windowHandlers.get("popstate")();
  await settle();
  selection.isCollapsed = true;
  await clickRow(jobs[0]);
  await settle();
  assert.equal(page.location.pathname, `/queues/main/jobs/${jobs[0].id}`);
});

// A queue page of `page.jobs` whose job name search suggests `names`, and lists `page.workers` from the next poll on.
// With `answers`, each request for names waits there until the test answers it.
const bootSearch = async (t, tag, names, { answers } = {}) => {
  const jobs = queueJobs(3);
  let page;
  page = await bootDashboard(t, {
    tag,
    path: "/queues/main",
    fetch: (url) => {
      if (url.includes("/job-names?")) {
        const answer = () => jsonResponse({ names });
        return answers ? new Promise((resolve) => answers.push(() => resolve(answer()))) : Promise.resolve(answer());
      }
      if (url.includes("/workers?")) return Promise.resolve(jsonResponse({ workers: page?.workers ?? [] }));
      if (url.includes("/jobs?")) return Promise.resolve(jsonResponse({ jobs }));
      const job = jobs.find(({ id }) => url.endsWith(id));
      return Promise.resolve(jsonResponse({ job: { ...job, queue: "main", kind: "job", priority: 0 } }));
    },
  });
  return Object.assign(page, { jobs, workers: [] });
};

// More names than the list's 14rem shows at once.
const sevenNames = Array.from({ length: 7 }, (_, index) => `job-${index}`);

// Types `query` into the job name search, with focus there, and runs the debounce that asks for suggestions.
const typeName = (page, query) => {
  const input = page.element("job-name-filter");
  input.focus();
  input.value = query;
  page.handlers.get("input")({ target: input });
  page.runTimers();
};

test("a name-suggestion answer opens no list once focus has left the search box", async (t) => {
  const answers = [];
  const page = await bootSearch(t, "suggestion-focus-gone-test", ["job-1", "job-2"], { answers });
  for (const [how, leave] of [
    ["a Tab on to the first job row", () => page.element(`row-entry-${page.jobs[0].id}`).focus()],
    // Nothing takes the focus over, as when a phone's keyboard is put away.
    ["focus leaving for nowhere", () => { page.document.activeElement = null; }],
  ]) {
    typeName(page, "job");
    leave();
    answers.shift()();
    await settle();
    assert.equal(page.list(), undefined, `the list opened after ${how}`);
    assert.equal(page.element("job-name-filter").getAttribute("aria-expanded"), "false");
  }
});

test("the name suggestions close when focus moves on, and neither a poll nor Back reopens them", async (t) => {
  const page = await bootSearch(t, "suggestion-focus-moves-test", ["job-1", "job-2"]);
  typeName(page, "job");
  await settle();
  const list = page.list();
  assert.ok(list, "the list never opened");

  // A Tab on to the first job row, which the open list would otherwise cover.
  page.element(`row-entry-${page.jobs[0].id}`).focus();
  assert.equal(list.removed, true, "the list stayed open behind the focus");
  assert.equal(page.element("job-name-filter").getAttribute("aria-expanded"), "false");
  page.workers = [{ id: "worker-1", stats: { complete: 1 } }];
  page.poll();
  await settle();
  assert.match(page.markup, /id="row-worker-worker-1"/, "the poll did not repaint");
  assert.equal(page.list(), undefined, "the poll painted the list again");

  // Back and Forward with the list open move no focus: the page itself goes, and the list with it.
  typeName(page, "job");
  await settle();
  assert.ok(page.list(), "the list never opened");
  page.location.pathname = `/queues/main/jobs/${page.jobs[0].id}`;
  page.windowHandlers.get("popstate")();
  await settle();
  page.location.pathname = "/queues/main";
  page.windowHandlers.get("popstate")();
  await settle();
  assert.match(page.markup, /class="page-content queue-page"/);
  assert.equal(page.list(), undefined, "Forward brought back a list nobody was using");
});

test("the name-suggestion list is no tab stop, and Escape closes it from the list too", async (t) => {
  const page = await bootSearch(t, "suggestion-list-focus-test", sevenNames);
  typeName(page, "job");
  await settle();
  const list = page.list();
  // Seven names overflow its 14rem, and Chrome and Firefox make a box that overflows a tab stop of its own.
  assert.equal(list.getAttribute("tabindex"), "-1");

  // A click on its scroll bar can still focus it, where the input's keys never arrive.
  list.focus();
  let prevented = false;
  page.handlers.get("keydown")({ target: list, key: "Escape", preventDefault() { prevented = true; } });
  assert.equal(list.removed, true, "Escape left the list open");
  assert.equal(prevented, true);
  assert.equal(page.document.activeElement, page.element("job-name-filter"), "focus was not handed back to the input");
  assert.equal(page.element("job-name-filter").getAttribute("aria-expanded"), "false");
});

test("the name suggestions open above the search box only where the page has no room for them below", async (t) => {
  const page = await bootSearch(t, "suggestion-direction-test", sevenNames);
  // A 1366x625 laptop, its page scrolled down to the jobs: `.page-content` spans 50-625 under the header, and the
  // search box sits at 188-221. The 201px list fits below it, where above it would open into the page's scrolled-away
  // top.
  page.layout = { port: { top: 50, bottom: 625 }, search: { top: 188, bottom: 221 }, list: 201 };
  typeName(page, "job");
  await settle();
  assert.equal(page.list().classList.contains("above"), false, "the list opened into the hidden top of the page");

  // With the box low on the page, 32px above its bottom edge and 510 below its top, the next repaint turns it up.
  page.layout.search = { top: 560, bottom: 593 };
  page.workers = [{ id: "worker-1", stats: { complete: 1 } }];
  page.poll();
  await settle();
  assert.match(page.markup, /id="row-worker-worker-1"/, "the poll did not repaint");
  assert.equal(page.list().classList.contains("above"), true, "the list hung off the bottom of the page");
});

test("no media query decides which way the name suggestions open", async () => {
  const stylesheet = await readFile(new URL("../../dashboard/app.css", import.meta.url), "utf8");
  // The room above and below the search box comes from where the page is scrolled, not the viewport's size: app.mjs
  // measures it, and turns the list up with a class.
  for (const { condition, rules } of mediaBlocks(stylesheet)) {
    for (const [selector, declarations] of rules) {
      if (!selector.includes(".name-suggestions")) continue;
      assert.ok(!("top" in declarations) && !("bottom" in declarations), `@media${condition}places ${selector}`);
    }
  }
  assert.match(stylesheet, /\n\.name-suggestions\.above\s*\{[^}]*\bbottom:/, "no rule turns the list up");
});
