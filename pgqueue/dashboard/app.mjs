export const cursorView = () => ({
  cursor: null,
  history: [],
  start: 1,
  nextCursor: null,
  pageCount: 0,
  limit: 25,
});

export const escapeHtml = (value) =>
  String(value).replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);

// 999_950, not 1_000_000: toFixed(1) rounds anything above it to "1000.0K".
export const compact = (value) => {
  if (value == null) return "–";
  if (value >= 999_950) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 10_000) return `${(value / 1_000).toFixed(1)}K`;
  return String(value);
};

export const duration = (milliseconds) => {
  if (milliseconds == null) return "–";
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  }
  return `${Math.floor(seconds / 86400)}d ${Math.floor((seconds % 86400) / 3600)}h`;
};

export const dashboardHome = (root) => root || "/";

export const dashboardUrl = (root, href) =>
  href === "/" ? dashboardHome(root) : root + href;

export const pageOf = (items, view) => {
  const lastOffset = items.length
    ? Math.floor((items.length - 1) / view.limit) * view.limit
    : 0;
  view.offset = Math.min(view.offset, lastOffset);
  return items.slice(view.offset, view.offset + view.limit);
};

export const resetCursor = (view) => {
  view.cursor = null;
  view.history = [];
  view.start = 1;
  view.nextCursor = null;
  view.pageCount = 0;
};

export const appendCursor = (params, cursor, timestampKey) => {
  if (!cursor) return;
  params.set(timestampKey, cursor.timestamp);
  params.set("cursor_id", cursor.id);
};

export const entryRequestKey = (view, kind) =>
  JSON.stringify([
    view.queue,
    kind,
    [...view.statuses].sort(),
    view.name,
    view.limit,
    view.cursor,
  ]);

const decodeSegment = (segment) => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
};

export const parseRoute = (pathname, root) => {
  let path = pathname;
  if (root && (path === root || path.startsWith(`${root}/`))) {
    path = path.slice(root.length) || "/";
  }

  const workerMatch = path.match(/^\/queues\/([^/]+)\/workers\/([^/]+)$/);
  if (workerMatch) {
    return {
      view: "worker",
      queue: decodeSegment(workerMatch[1]),
      id: workerMatch[2],
    };
  }

  const jobMatch = path.match(/^\/queues\/([^/]+)\/jobs\/([^/]+)$/);
  if (jobMatch) {
    return {
      view: "job",
      queue: decodeSegment(jobMatch[1]),
      id: jobMatch[2],
    };
  }

  const queueMatch = path.match(/^\/queues\/([^/]+)$/);
  if (queueMatch) {
    return { view: "queue", queue: decodeSegment(queueMatch[1]), id: null };
  }

  return { view: "home", queue: null, id: null };
};

// Job names are at most 255 UTF-8 bytes — enqueue refuses a longer one, and the listing refuses a longer filter — so a
// filter past that can match nothing.
export const exceedsJobNameLimit = (name) => new TextEncoder().encode(name).length > 255;

export const isSuggestionResponseCurrent = (view, requestId, prefix) =>
  view.suggestionRequest === requestId &&
  (prefix === undefined || view.query.trim() === prefix);

/* pgqueue dashboard — no-build-step vanilla JS SPA.
 * Routes:  /            queues overview
 *          /queues/:q          queue detail (workers, jobs, crons)
 *          /queues/:q/workers/:id worker detail
 *          /queues/:q/jobs/:id job detail (payload/result, retry/abort)
 * Data refreshes every 5s while visible; navigation uses pushState.
 */
const startDashboard = () => {
  "use strict";

  const ROOT = document.querySelector('meta[name="pgqueue-root"]')?.content || "";
  const DASHBOARD_USER = document.querySelector('meta[name="pgqueue-user"]')?.content || "anonymous";
  const AUTH_ENABLED = document.querySelector('meta[name="pgqueue-auth-enabled"]')?.content === "true";
  const DASHBOARD_TIME_ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const app = document.getElementById("app");
  const REFRESH_MS = 5000;
  const ACCOUNT_MESSAGE_MS = 10000;

  // Module-level so it survives repaints: render() replaces all of #app, so no
  // per-table state can live in the DOM. Reset only when the queue changes.
  const queuesView = { name: "", offset: 0, limit: 25 };
  const workersView = { queue: null, ...cursorView() };
  const createEntryView = () => ({
    queue: null,
    ...cursorView(),
    statuses: new Set(),
    name: "",
    query: "",
    suggestions: [],
    suggestionIndex: -1,
    suggestionsOpen: false,
    // Monotonic, and deliberately not reset: it is what lets a landing response
    // recognise that it has been superseded.
    suggestionRequest: 0,
  });
  const entries = {
    job: { key: "jobs", view: createEntryView() },
    cron: { key: "crons", view: createEntryView() },
  };
  const activeEntry = () => entries[entryKind];
  const scrollPositions = new Map();
  let entryKind = "job";
  let queueSearchTimer;
  let suggestionTimer;
  let accountMessageTimer;

  const esc = escapeHtml;

  const requestJson = async (path, options = {}) => {
    const response = await fetch(`${ROOT}/api${path}`, {
      ...options,
      // On reads too, not only on the actions that require it: the server answers a request carrying it without a Basic
      // challenge, which the browser would otherwise meet with its own credentials dialog in place of the redirect
      // below.
      headers: { "X-PGQueue-Request": "dashboard", ...options.headers },
    });
    // Without built-in authentication there is no login form to go to: the 401 came from the application's own
    // middleware, so it fails like any other request, and a reload by the user hands the page back to that middleware.
    // Reloading automatically would loop forever behind a middleware that guards only the API.
    if (response.status === 401 && AUTH_ENABLED) {
      // Never resolves, deliberately: the redirect is already under way and no caller should run on. Session expired,
      // or a password change killed it.
      window.location.assign(`${ROOT}/login`);
      return new Promise(() => {});
    }
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.error || `${response.status} ${response.statusText}`);
    }
    return response.json();
  };

  const api = (path) => requestJson(path);

  const post = (path, payload) =>
    requestJson(path, {
      method: "POST",
      headers: payload === undefined ? {} : { "Content-Type": "application/json" },
      body: payload === undefined ? undefined : JSON.stringify(payload),
    });

  // A `stats` document past the server's body cap arrives as the leading
  // characters of the stored JSON — a string, not an object — so `stats?.complete`
  // and every sibling below read `undefined` and render "–", indistinguishable
  // from a worker that reported nothing. `Consumer::heartbeat` is a public writer
  // of `stats`, so the cap is reachable, and `stats_truncated` is the server
  // saying which of the two it is: this is where that flag reaches the screen,
  // as jsonBlock does for `metadata`.
  const statCell = (worker, key, render = compact) =>
    worker.stats_truncated
      ? '<span class="truncated" title="stats exceeded the response cap">… truncated</span>'
      : esc(render(worker.stats?.[key]));

  // calendar/numberingSystem are load-bearing: whenText pastes these parts into
  // a fixed YYYY-MM-DD, and a locale default would render th-TH as Buddhist 2569
  // or fa-IR in Persian digits, disagreeing with the ISO `datetime=` when()
  // writes on the same element.
  /** @type {Intl.DateTimeFormatOptions} */
  const calendarOptions = {
    calendar: "gregory",
    numberingSystem: "latn",
    timeZone: DASHBOARD_TIME_ZONE,
  };

  /** @type {Intl.DateTimeFormatOptions} */
  const dateOptions = {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    ...calendarOptions,
  };
  const dateFormat = new Intl.DateTimeFormat(undefined, dateOptions);

  /** @type {Intl.DateTimeFormatOptions} */
  const timeOptions = {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    ...calendarOptions,
  };
  const timeFormat = new Intl.DateTimeFormat(undefined, timeOptions);

  // Rust timestamps can include years outside 1..=9999, which ECMAScript
  // rejects, and every Intl formatter throws RangeError on that Invalid Date. That
  // escapes into render()'s catch, which the 5s poll then rebuilds forever, so
  // one bad timestamp would take every route down. Both helpers stay total.
  const parsed = (iso) => {
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const whenText = (iso) => {
    if (!iso) return "–";
    const date = parsed(iso);
    if (!date) return iso;
    const parts = Object.fromEntries(
      dateFormat
        .formatToParts(date)
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, part.value]),
    );
    const timeParts = Object.fromEntries(
      timeFormat
        .formatToParts(date)
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, part.value]),
    );
    return `${parts.year}-${parts.month}-${parts.day} ${timeParts.hour}:${timeParts.minute}:${timeParts.second}`;
  };

  const when = (iso) => {
    if (!iso) return "–";
    const date = parsed(iso);
    if (!date) return `<time class="timestamp">${esc(iso)}</time>`;
    return `<time class="timestamp" datetime="${esc(date.toISOString())}">${esc(whenText(iso))}</time>`;
  };

  const statusBadge = (status) =>
    `<span class="status ${esc(status)}"><span class="dot"></span>${esc(status)}</span>`;

  // A truncated body arrives as the leading characters of the stored JSON, as
  // a string, because the server refuses to move an unbounded payload off the
  // database. Re-encoding that string as JSON would quote and escape it, so it
  // is printed as it came, with the cut called out.
  const jsonBlock = (value, truncated) =>
    truncated
      ? `<pre class="blob">${esc(value)}<span class="truncated">… truncated</span></pre>`
      : `<pre class="blob">${value == null ? "–" : esc(JSON.stringify(value, null, 2))}</pre>`;

  // Stored errors are capped at 1 MiB and this route cuts them further for the
  // browser. They are already strings, so only the visible cut is new.
  const errorBlock = (value, truncated) => {
    if (!value) return "–";
    const cut = truncated ? '<span class="truncated">… truncated</span>' : "";
    return `<span class="error-banner">${esc(value)}${cut}</span>`;
  };

  // `value` is interpolated as HTML, because most callers pass a fragment they
  // built themselves (statusBadge, jsonBlock, errorBlock, a <span class="mono">).
  // Escaping is therefore the caller's job, and every call site that passes
  // server data directly wraps it in esc(). This is the only place in this file
  // where that responsibility inverts, so it is the one to check when adding a
  // row: an unescaped job name or dedupe key here is stored XSS.
  const detailRow = (label, value) =>
    `<tr><th scope="row">${label}</th><td>${value}</td></tr>`;

  const url = (href) => dashboardUrl(ROOT, href);

  // render() replaces all of #app, so focus survives a repaint only by id.
  // Every focusable control emitted by this file needs one, derived from its own
  // data so it is stable across repaints and across list reordering. Parts are
  // percent-encoded because ids may not contain whitespace and queue names may.
  const domId = (...parts) => parts.map((part) => encodeURIComponent(part)).join("-");

  // `href` and `id` are escaped here; `text` is not, for the same reason
  // detailRow's `value` is not — callers pass markup (a <span class="mono">
  // wrapper) as often as a bare string. Every current call site escapes it.
  const link = (href, text, id) =>
    `<a href="${esc(url(href))}" id="${esc(id)}" data-nav>${text}</a>`;

  const breadcrumb = (href, label) => {
    const safeLabel = esc(label);
    const content = label === "PGQueue"
      ? `<img class="breadcrumb-logo" src="${esc(url("/static/favicon.svg"))}" alt="" width="20" height="20" ` +
        `aria-hidden="true"><span>${safeLabel}</span>`
      : safeLabel;
    return href
      ? `<li><a href="${esc(url(href))}" id="${esc(domId("breadcrumb", href))}" data-nav title="${safeLabel}">${content}</a></li>`
      : `<li><span aria-current="page" title="${safeLabel}">${content}</span></li>`;
  };

  const rowNavAttrs = (href, id) =>
    `class="clickable-row" id="${esc(id)}" data-row-nav="${esc(url(href))}" tabindex="0"`;

  const pagerMarkup = (name, first, last, hasPrevious, hasNext) => {
    return `<div class="pager">
      <span class="pager-summary">Showing ${first}-${last}</span>
      <div class="pager-controls">
        <button type="button" class="outline" id="${esc(domId("pager", name, "prev"))}"
                data-pager="${esc(name)}" data-page="-1"
                ${hasPrevious ? "" : "disabled"}>Previous</button>
        <button type="button" class="outline" id="${esc(domId("pager", name, "next"))}"
                data-pager="${esc(name)}" data-page="1"
                ${hasNext ? "" : "disabled"}>Next</button>
      </div>
    </div>`;
  };

  const accountControls = () => AUTH_ENABLED
    ? `<span class="account-message" role="status" aria-live="polite"></span>
      <details class="account-menu">
        <summary id="account-summary" title="Account actions">${esc(DASHBOARD_USER)}</summary>
        <ul>
          <li><button type="button" id="account-action-password" data-account-action="password">Change password</button></li>
          <li><button type="button" id="account-action-logout" data-account-action="logout">Log out</button></li>
        </ul>
      </details>`
    : `<span title="Signed-in user">${esc(DASHBOARD_USER)}</span>`;

  const passwordDialog = () => AUTH_ENABLED
    ? `<dialog class="account-dialog" id="password-dialog">
        <article>
          <form id="password-form">
            <header><h2>Change password</h2></header>
            <label for="current-password">Current password</label>
            <input id="current-password" name="current_password" type="password"
                   autocomplete="current-password" required>
            <label for="new-password">New password</label>
            <input id="new-password" name="new_password" type="password"
                   autocomplete="new-password" minlength="8" required>
            <label for="confirm-password">Confirm new password</label>
            <input id="confirm-password" name="confirm_password" type="password"
                   autocomplete="new-password" minlength="8" required>
            <p class="form-error" role="alert"></p>
            <footer>
              <button type="button" class="secondary" id="password-cancel" data-account-action="cancel-password">Cancel</button>
              <button type="submit" id="password-submit">Change password</button>
            </footer>
          </form>
        </article>
      </dialog>`
    : "";

  const layout = (crumbs, body, pageClass = "") => `
    <header class="app-header">
      <nav class="breadcrumbs" aria-label="Breadcrumb">
        <ol>${crumbs.join("")}</ol>
      </nav>
      <div class="nav-context" aria-label="Dashboard context">
        ${accountControls()}
      </div>
    </header>
    ${passwordDialog()}
    <div class="page-content ${esc(pageClass)}">${body}</div>`;

  const signalBadge = (value, tone = "") =>
    `<span class="signal${tone ? ` ${esc(tone)}` : ""}">${esc(value)}</span>`;

  const executionTone = (execution) => execution === "running"
    ? "running"
    : execution === "aborting" ? "aborted" : "";

  const homeView = async () => {
    const { queues } = await api("/queues");
    const queueNeedle = queuesView.name.trim().toLowerCase();
    const filteredQueues = queueNeedle
      ? queues.filter((queue) => queue.name.toLowerCase().includes(queueNeedle))
      : queues;
    const visibleQueues = pageOf(filteredQueues, queuesView);
    const rows = visibleQueues
      .map(
        (q) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(q.name)}`, domId("row-queue", q.name))}>
          <td>${link(`/queues/${encodeURIComponent(q.name)}`, esc(q.name), domId("link-queue", q.name))}</td>
          <td>${q.oldest_ready_at ? when(q.oldest_ready_at) : signalBadge("Idle")}</td>
          <td class="queue-state">${signalBadge(q.execution, executionTone(q.execution))}</td>
          <td>${q.next_scheduled_at ? when(q.next_scheduled_at) : "–"}</td>
          <td class="queue-state">${signalBadge(q.has_live_workers ? "Online" : "None", q.has_live_workers ? "complete" : "failed")}</td>
          <td>${q.latest_failure_or_abort_at ? when(q.latest_failure_or_abort_at) : "–"}</td>
        </tr>`,
      )
      .join("");
    return layout(
      [breadcrumb(null, "PGQueue")],
      `<section class="table-section queues-section">
        <h2>Queues</h2>
        <form class="table-search search-filter" aria-label="Search queues">
          <div class="search-field">
            <input type="search" id="queue-name-filter" placeholder="Search queue names"
                   aria-label="Search queues by name" autocomplete="off" spellcheck="false"
                   value="${esc(queuesView.name)}">
          </div>
        </form>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="queues"><table class="data-table">
            <thead><tr><th>Name</th><th>Ready since</th><th>Execution</th>
            <th>Next scheduled</th><th>Workers</th><th>Latest failure/abort</th></tr></thead>
            <tbody>${rows || '<tr class="empty-row"><td colspan="6">No matching queues.</td></tr>'}</tbody>
          </table></div>
          ${pagerMarkup(
            "queues",
            visibleQueues.length ? queuesView.offset + 1 : 0,
            queuesView.offset + visibleQueues.length,
            queuesView.offset > 0,
            queuesView.offset + visibleQueues.length < filteredQueues.length,
          )}
        </div>
      </section>`,
      "home-page",
    );
  };

  const STATUSES = ["queued", "running", "complete", "failed", "aborting", "aborted"];

  const queueView = async (name) => {
    const { view: entryView, key: entryKey } = activeEntry();
    // Captured, not re-read after the await: a Cron tab click landing mid-flight
    // would otherwise paint job rows under the cron header and pager.
    const kind = entryKind;
    if (entryView.queue !== name) {
      resetEntryView(entryView);
      entryView.queue = name;
    }
    const params = new URLSearchParams();
    if (entryView.statuses.size) params.set("status", [...entryView.statuses].join(","));
    if (entryView.name) params.set("name", entryView.name);
    params.set("kind", kind);
    params.set("limit", entryView.limit);
    appendCursor(params, entryView.cursor, "cursor_enqueued_at");

    if (workersView.queue !== name) {
      workersView.queue = name;
      resetCursor(workersView);
    }
    const workerParams = new URLSearchParams({ limit: String(workersView.limit) });
    appendCursor(workerParams, workersView.cursor, "cursor_started_at");

    const issuedEntryRequest = entryRequestKey(entryView, kind);
    const issuedWorkerCursor = workersView.cursor;

    const [workerPage, jobPage] = await Promise.all([
      api(`/queues/${encodeURIComponent(name)}/workers?${workerParams}`),
      // Answered here rather than asked: the listing refuses such a filter outright, and that refusal replaced the
      // whole page — search box included — on every poll until a reload.
      exceedsJobNameLimit(entryView.name)
        ? { jobs: [], next_cursor: null }
        : api(`/queues/${encodeURIComponent(name)}/jobs?${params}`),
    ]);
    // The whole response is dropped, not just its cursor, or rows and pager get
    // built from state that has already moved. `null` is a contract with
    // render(): discard this paint and reissue.
    if (
      entryRequestKey(entryView, kind) !== issuedEntryRequest
      || workersView.cursor !== issuedWorkerCursor
    ) {
      return null;
    }
    const { workers } = workerPage;
    const { jobs } = jobPage;
    const nextWorkerCursor = workerPage["next_cursor"];
    const nextJobCursor = jobPage["next_cursor"];
    workersView.nextCursor = nextWorkerCursor
      ? { timestamp: nextWorkerCursor.started_at, id: nextWorkerCursor.id }
      : null;
    workersView.pageCount = workers.length;
    entryView.nextCursor = nextJobCursor
      ? { timestamp: nextJobCursor.enqueued_at, id: nextJobCursor.id }
      : null;
    entryView.pageCount = jobs.length;

    const workerRows = workers
      .map(
        (w) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(name)}/workers/${w.id}`, domId("row-worker", w.id))}>
          <td class="mono job-id">${link(
            `/queues/${encodeURIComponent(name)}/workers/${w.id}`,
            esc(w.id),
            domId("link-worker", w.id),
          )}</td>
          <td class="num">${statCell(w, "complete")}</td>
          <td class="num">${statCell(w, "retried")}</td>
          <td class="num">${statCell(w, "failed")}</td>
          <td class="num">${statCell(w, "aborted")}</td>
          <td class="num">${statCell(w, "uptime_ms", duration)}</td>
        </tr>`,
      )
      .join("");

    // data-entry-kind, not activeEntry(): the kind tab flips the active kind
    // before the render lands, so for one round trip the markup on screen
    // belongs to the previous kind. Handlers resolve the view from the target.
    const tabs = STATUSES.map((status) => {
      const selected = entryView.statuses.has(status);
      return `<button type="button" id="${esc(domId("status-filter", kind, status))}" data-entry-kind="${esc(kind)}" data-status="${status}" aria-pressed="${selected}"><span class="status ${esc(status)}"><span class="dot"></span>${esc(status)}</span></button>`;
    }).join("");

    const suggestionsOpen = entryView.suggestionsOpen && entryView.suggestions.length > 0;
    const suggestionId = (index) => `${entryKey.slice(0, -1)}-name-suggestion-${index}`;
    // role="presentation" on the <li> because a listbox may only own options,
    // and tabindex="-1" because an aria-activedescendant combobox keeps focus on
    // the input — the arrows and Escape listen there and nowhere else.
    const suggestions = suggestionsOpen
      ? `<ul class="name-suggestions" id="job-name-suggestions" role="listbox">${entryView.suggestions.map((suggestion, index) =>
        `<li role="presentation"><button type="button" role="option" id="${esc(suggestionId(index))}" data-entry-kind="${esc(kind)}" data-job-name="${esc(suggestion)}" tabindex="-1" aria-selected="${index === entryView.suggestionIndex}">${esc(suggestion)}</button></li>`,
      ).join("")}</ul>`
      : "";
    // aria-controls and aria-activedescendant are emitted only alongside the
    // list they name, and the index is bounded by the array actually painted:
    // the arrows clamp to the options on screen, which can briefly outnumber it.
    const activeSuggestion = suggestionsOpen
      && entryView.suggestionIndex >= 0
      && entryView.suggestionIndex < entryView.suggestions.length
      ? ` aria-activedescendant="${esc(suggestionId(entryView.suggestionIndex))}"`
      : "";

    const rows = jobs
      .map(
        (j) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(name)}/jobs/${j.id}`, domId("row-entry", j.id))}>
          <td class="mono job-id">${link(
            `/queues/${encodeURIComponent(name)}/jobs/${j.id}`,
            esc(j.id),
            domId("link-entry", j.id),
          )}</td>
          <td class="job-name"><span title="${esc(j.name)}">${esc(j.name)}</span></td>
          ${kind === "cron" ? `<td class="mono cron-expression">${esc(j.cron_expr ?? "–")}</td>` : ""}
          <td><button type="button" class="status status-chip ${esc(j.status)}" id="${esc(domId("status-chip", j.id))}" data-entry-kind="${esc(kind)}" data-status="${esc(j.status)}" title="Filter by ${esc(j.status)}"><span class="dot"></span>${esc(j.status)}</button></td>
          <td class="num">${j.attempts}/${j.max_attempts}</td>
          <td>${when(j.scheduled_at)}</td>
          <td>${when(j.completed_at)}</td>
        </tr>`,
      )
      .join("");

    return layout(
      [breadcrumb("/", "PGQueue"), breadcrumb(null, `Queue ${name}`)],
      `<section class="table-section workers-section">
        <h2>Workers</h2>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="workers"><table class="data-table">
            <thead><tr><th>Worker</th><th class="num">Complete</th><th class="num">Retried</th>
            <th class="num">Failed</th><th class="num">Aborted</th><th class="num">Uptime</th></tr></thead>
            <tbody>${workerRows || '<tr class="empty-row"><td colspan="6">No results found.</td></tr>'}</tbody>
          </table></div>
          ${pagerMarkup(
            "workers",
            workers.length ? workersView.start : 0,
            workers.length ? workersView.start + workers.length - 1 : 0,
            workersView.history.length > 0,
            Boolean(workersView.nextCursor),
          )}
        </div>
      </section>
      <section class="jobs-section">
        <h2>Jobs</h2>
        <div class="job-toolbar">
          <div class="segmented kind-tabs" role="tablist" aria-label="Queue entries">
            <button type="button" role="tab" id="kind-tab-job" data-kind="job" aria-selected="${kind === "job"}">One-Off</button>
            <button type="button" role="tab" id="kind-tab-cron" data-kind="cron" aria-selected="${kind === "cron"}">Cron</button>
          </div>
          <div class="filter-group">
            <div class="segmented" role="group" aria-label="Filter jobs by status">${tabs}</div>
          </div>
          <form class="search-filter job-name-search" data-entry-kind="${esc(kind)}" aria-label="Search by job name">
            <div class="search-field">
              <input type="search" id="${entryKey.slice(0, -1)}-name-filter" placeholder="Search by job name"
                     aria-label="Search by job name" autocomplete="off" spellcheck="false"
                     role="combobox" aria-expanded="${suggestionsOpen}"
                     ${suggestionsOpen ? 'aria-controls="job-name-suggestions"' : ""}${activeSuggestion}
                     value="${esc(entryView.query)}">
            </div>
            ${suggestions}
          </form>
        </div>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="${entryKey}"><table class="data-table">
            <thead><tr><th>ID</th><th>Name</th>${kind === "cron" ? "<th>Schedule</th>" : ""}<th>Status</th><th class="num">Attempts</th>
            <th>${kind === "cron" ? "Next run" : "Scheduled"}</th><th>Completed</th></tr></thead>
            <tbody>${rows || `<tr class="empty-row"><td colspan="${kind === "cron" ? 7 : 6}">No jobs found.</td></tr>`}</tbody>
          </table></div>
          ${pagerMarkup(
            entryKey,
            jobs.length ? entryView.start : 0,
            jobs.length ? entryView.start + jobs.length - 1 : 0,
            entryView.history.length > 0,
            Boolean(entryView.nextCursor),
          )}
        </div>
      </section>`,
      "queue-page",
    );
  };

  const workerView = async (name, id) => {
    const { worker } = await api(`/queues/${encodeURIComponent(name)}/workers/${id}`);

    return layout(
      [
        breadcrumb("/", "PGQueue"),
        breadcrumb(`/queues/${encodeURIComponent(name)}`, `Queue ${name}`),
        breadcrumb(null, `Worker ${worker.id}`),
      ],
      `<div class="detail-heading"><h2>Worker ${esc(worker.id)}</h2></div>
      <div class="table-frame"><div class="table-scroll" data-scroll-key="worker-details"><table class="data-table">
        ${detailRow("ID", `<span class="mono">${esc(worker.id)}</span>`)}
        ${detailRow("Queue", esc(worker.queue))}
        ${detailRow("Complete", statCell(worker, "complete"))}
        ${detailRow("Retried", statCell(worker, "retried"))}
        ${detailRow("Failed", statCell(worker, "failed"))}
        ${detailRow("Aborted", statCell(worker, "aborted"))}
        ${detailRow("Uptime", statCell(worker, "uptime_ms", duration))}
        ${detailRow("Started", when(worker.started_at))}
        ${detailRow("Last heartbeat", when(worker.heartbeat_at))}
        ${detailRow("Expires", when(worker.expires_at))}
        ${detailRow("Metadata", jsonBlock(worker.metadata, worker.metadata_truncated))}
      </table></div></div>`,
      "detail-page",
    );
  };

  const jobView = async (name, id) => {
    const { job } = await api(`/queues/${encodeURIComponent(name)}/jobs/${id}`);
    const isCron = job.kind === "cron";
    const detailLabel = isCron ? "Cron" : "Job";
    const terminal = ["complete", "failed", "aborted"].includes(job.status);
    const abortable = ["queued", "running"].includes(job.status);

    return layout(
      [
        breadcrumb("/", "PGQueue"),
        breadcrumb(`/queues/${encodeURIComponent(name)}`, `Queue ${name}`),
        breadcrumb(null, `${detailLabel} ${job.id}`),
      ],
      `<div class="detail-heading">
        <h2>${detailLabel} ${esc(job.id)}</h2>
        <div class="actions">
          <button type="button" id="action-retry" data-action="retry" ${terminal ? "" : "disabled"}>Retry</button>
          <button type="button" class="secondary" id="action-abort" data-action="abort" ${abortable ? "" : "disabled"}>Abort</button>
        </div>
      </div>
      <div class="action-message" role="status" aria-live="polite" hidden>
        <span class="action-message-text"></span>
        <button type="button" class="secondary" id="action-message-dismiss"
                data-dismiss-action-message aria-label="Dismiss message">Dismiss</button>
      </div>
      <div class="table-frame"><div class="table-scroll" data-scroll-key="job-details"><table class="data-table">
        ${detailRow("ID", `<span class="mono">${esc(job.id)}</span>`)}
        ${detailRow("Name", esc(job.name))}
        ${isCron ? detailRow("Schedule", `<span class="mono cron-expression">${esc(job.cron_expr ?? "–")}</span>`) : ""}
        ${detailRow("Status", statusBadge(job.status))}
        ${detailRow("Attempts", `${job.attempts}/${job.max_attempts}`)}
        ${detailRow("Priority", job.priority)}
        ${detailRow("Dedupe key", esc(job.dedupe_key ?? "–"))}
        ${detailRow("Enqueued", when(job.enqueued_at))}
        ${detailRow("Scheduled", when(job.scheduled_at))}
        ${detailRow("Started", when(job.started_at))}
        ${detailRow("Completed", when(job.completed_at))}
        ${detailRow("Last updated", when(job.updated_at))}
        ${detailRow("Worker", `<span class="mono">${esc(job.worker_id ?? "–")}</span>`)}
        ${detailRow("Error", errorBlock(job.error, job.error_truncated))}
        ${detailRow("Payload", jsonBlock(job.payload, job.payload_truncated))}
        ${detailRow("Result", jsonBlock(job.result, job.result_truncated))}
        ${detailRow("Metadata", jsonBlock(job.meta, job.meta_truncated))}
      </table></div></div>`,
      "detail-page",
    );
  };

  const errorView = (error) =>
    layout(
      [breadcrumb("/", "PGQueue"), breadcrumb(null, "Error")],
      `<article class="error-banner">${esc(error.message || error)}</article>`,
    );

  const route = () => {
    const current = parseRoute(location.pathname, ROOT);
    if (current.view === "worker") {
      return { ...current, render: () => workerView(current.queue, current.id) };
    }
    if (current.view === "job") {
      return { ...current, render: () => jobView(current.queue, current.id) };
    }
    if (current.view === "queue") {
      return { ...current, render: () => queueView(current.queue) };
    }
    return { ...current, render: homeView };
  };

  let rendering = false;
  let renderRequested = false;
  let lastMarkup = null;
  let renderedPath = null;
  // Module-level, not on the button: a repaint replaces the button mid-request.
  let actionInFlight = false;
  const render = async () => {
    // Deferred, not dropped, so the view re-syncs when the dialog closes.
    if (rendering || app.querySelector(".account-dialog[open]")) {
      renderRequested = true;
      return;
    }
    rendering = true;
    try {
      // route() reads nothing but the path, so the path is the whole identity of
      // the requested view — a navigation mid-flight invalidates the response.
      const issuedPath = location.pathname;
      const nextMarkup = await route().render();
      if (location.pathname !== issuedPath) {
        renderRequested = true;
        return;
      }
      if (nextMarkup == null) {
        renderRequested = true;
        return;
      }
      if (app.querySelector(".account-dialog[open]")) {
        renderRequested = true;
        return;
      }
      if (nextMarkup !== lastMarkup) {
        const active = document.activeElement;
        const focusId = active?.id;
        const selectionStart = active?.selectionStart;
        const selectionEnd = active?.selectionEnd;
        // The text those offsets index into: committing a suggestion rewrites
        // the input, so the offsets are only replayable onto unchanged text.
        const selectionValue = active?.value;
        app.querySelectorAll("[data-scroll-key]").forEach((element) => {
          scrollPositions.set(element.dataset.scrollKey, {
            left: element.scrollLeft,
            top: element.scrollTop,
          });
        });
        // None of this is in the generated markup — the browser sets <details
        // open> itself and both message nodes are written imperatively — so a
        // poll would otherwise wipe them. The nodes are moved, not copied, so
        // re-inserting announces nothing new to the live region. Their lifetime
        // is owned elsewhere (setAccountMessage's timer, navigate, popstate),
        // since this restore alone would carry them forever.
        const accountMenuOpen = app.querySelector(".account-menu")?.hasAttribute("open");
        const accountMessage = app.querySelector(".account-message");
        const actionMessage = app.querySelector(".action-message:not([hidden])");
        app.innerHTML = nextMarkup;
        lastMarkup = nextMarkup;
        if (accountMenuOpen) app.querySelector(".account-menu")?.setAttribute("open", "");
        if (accountMessage?.textContent) {
          app.querySelector(".account-message")?.replaceWith(accountMessage);
        }
        if (actionMessage) {
          app.querySelector(".action-message")?.replaceWith(actionMessage);
        }
        // `disabled` is not in the markup for a retryable job, so a repaint
        // mid-request would draw live-looking buttons actionInFlight refuses.
        if (actionInFlight) {
          app.querySelectorAll("button[data-action]").forEach((button) => {
            button.disabled = true;
          });
        }
        app.querySelectorAll("[data-scroll-key]").forEach((element) => {
          const position = scrollPositions.get(element.dataset.scrollKey);
          if (position?.left) element.scrollLeft = position.left;
          if (position?.top) element.scrollTop = position.top;
        });
        let nextActive = focusId ? document.getElementById(focusId) : null;
        // focus() on a disabled element is a no-op, so finding it is not enough
        // — focus would fall to <body>. A pager reaches this by working normally
        // (Next on the last page), and its sibling takes the paging over.
        if (nextActive?.disabled) {
          nextActive = nextActive.closest(".pager-controls")
            ?.querySelector("button:not(:disabled)") ?? null;
        }
        if (nextActive) {
          nextActive.focus({ preventScroll: true });
          if (selectionStart != null && selectionEnd != null) {
            if (nextActive.value === selectionValue) {
              nextActive.setSelectionRange(selectionStart, selectionEnd);
            } else {
              const caret = nextActive.value.length;
              nextActive.setSelectionRange(caret, caret);
            }
          }
        }
        // The list is rebuilt as a fresh <ul>, so its scroll offset goes with the
        // old one and paintSuggestionSelection's scrollIntoView never re-runs.
        // Read from the DOM: this markup may belong to the non-active kind.
        app
          .querySelector('.name-suggestions [role=option][aria-selected="true"]')
          ?.scrollIntoView({ block: "nearest" });
      }
      renderedPath = issuedPath;
    } catch (error) {
      if (app.querySelector(".account-dialog[open]")) {
        renderRequested = true;
        return;
      }
      app.innerHTML = errorView(error);
      lastMarkup = null;
      renderedPath = null;
    } finally {
      rendering = false;
      if (renderRequested) {
        renderRequested = false;
        void render();
      }
    }
  };

  const resetEntryView = (view) => {
    view.statuses.clear();
    view.name = "";
    view.query = "";
    view.suggestions = [];
    view.suggestionIndex = -1;
    view.suggestionsOpen = false;
    resetCursor(view);
  };

  // Filters and cursors deliberately survive navigation; queueView resets them
  // when the queue changes. These two must not: an armed debounce would fire
  // from a page with no search box, and an action message would be carried by
  // render() into the next job page's slot.
  const navigate = (url) => {
    for (const { view } of Object.values(entries)) cancelSuggestionFetch(view);
    clearActionMessage();
    history.pushState(null, "", url);
    void render();
  };

  const resetTableScroll = (name) => {
    scrollPositions.set(name, { left: 0, top: 0 });
    const table = app.querySelector(`[data-scroll-key="${name}"]`);
    if (table) {
      table.scrollLeft = 0;
      table.scrollTop = 0;
    }
  };

  // Both halves are needed: clearTimeout covers the armed debounce, and bumping
  // the id is what makes the landing guard reject a request already on the wire
  // — otherwise a dismissed list reopens itself when the response arrives.
  const cancelSuggestionFetch = (view) => {
    clearTimeout(suggestionTimer);
    view.suggestionRequest += 1;
  };

  // `name` is committed exactly as given: the listing's filter is an exact match and a job name may carry edge
  // whitespace, so trimming a picked suggestion filtered the listing to nothing. A typed query is trimmed by its
  // caller.
  const chooseJobName = (view, key, name) => {
    cancelSuggestionFetch(view);
    view.name = name;
    view.query = view.name;
    view.suggestions = [];
    view.suggestionIndex = -1;
    view.suggestionsOpen = false;
    resetCursor(view);
    resetTableScroll(key);
    // The clicked option's list is gone by the next paint, so focus needs an id
    // that still exists for render() to restore it to.
    suggestionCombobox()?.focus();
    void render();
  };

  const modifiedClick = (event) =>
    event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0;

  const suggestionCombobox = () => app.querySelector(".job-name-search input[role=combobox]");

  // Not interchangeable with view.suggestions: loadJobNameSuggestions replaces
  // that array a render ahead of the markup. The arrows clamp to this list, the
  // paint marks it, and Enter commits from it.
  const suggestionOptions = () => app.querySelectorAll(".name-suggestions [role=option]");

  // lastMarkup goes with the list: the DOM no longer matches what render() last
  // painted, so identical next markup would compare equal and be skipped. Only
  // when something was removed — this runs on every click outside the search.
  const removeSuggestionList = () => {
    const list = app.querySelector(".name-suggestions");
    if (!list) return;
    list.remove();
    const input = suggestionCombobox();
    if (input) {
      input.setAttribute("aria-expanded", "false");
      input.removeAttribute("aria-controls");
      input.removeAttribute("aria-activedescendant");
    }
    lastMarkup = null;
  };

  const paintSuggestionSelection = (view) => {
    const options = suggestionOptions();
    options.forEach((option, index) => {
      option.setAttribute("aria-selected", String(index === view.suggestionIndex));
    });
    const input = suggestionCombobox();
    if (!input) return;
    const active = options[view.suggestionIndex];
    if (active) {
      input.setAttribute("aria-activedescendant", active.id);
      // The list is capped at 14rem while the server returns up to twenty names.
      active.scrollIntoView({ block: "nearest" });
    } else {
      input.removeAttribute("aria-activedescendant");
    }
  };

  const clearAccountMessage = () => {
    clearTimeout(accountMessageTimer);
    const status = app.querySelector(".account-message");
    if (!status) return;
    status.classList.remove("error");
    status.textContent = "";
  };

  const setAccountMessage = (text, isError) => {
    clearTimeout(accountMessageTimer);
    const status = app.querySelector(".account-message");
    if (!status) return;
    status.classList.toggle("error", isError);
    status.textContent = text;
    // The only thing that ever clears it: render() moves this node across every
    // repaint, so nothing in the render path resets it.
    accountMessageTimer = setTimeout(clearAccountMessage, ACCOUNT_MESSAGE_MS);
  };

  const clearActionMessage = () => {
    const message = app.querySelector(".action-message");
    if (!message) return;
    message.hidden = true;
    message.classList.remove("error");
    message.querySelector(".action-message-text").textContent = "";
  };

  const setActionMessage = (text, isError) => {
    const message = app.querySelector(".action-message");
    if (!message) return;
    message.classList.toggle("error", isError);
    message.querySelector(".action-message-text").textContent = text;
    message.hidden = false;
  };

  const loadJobNameSuggestions = async (view, kind) => {
    const prefix = view.query.trim();
    const requestId = view.suggestionRequest + 1;
    view.suggestionRequest = requestId;
    if (!prefix) {
      view.suggestions = [];
      view.suggestionsOpen = false;
      void render();
      return;
    }
    const { queue } = route();
    if (!queue) return;
    const params = new URLSearchParams({ kind, prefix });
    // The same status filter the listing sends, so a suggestion can never name
    // a job the listing beside it then filters away.
    if (view.statuses.size) params.set("status", [...view.statuses].join(","));
    try {
      const { names } = await api(
        `/queues/${encodeURIComponent(queue)}/job-names?${params}`,
      );
      if (!isSuggestionResponseCurrent(view, requestId, prefix)) return;
      view.suggestions = names;
      view.suggestionIndex = -1;
      view.suggestionsOpen = true;
      void render();
    } catch {
      if (!isSuggestionResponseCurrent(view, requestId)) return;
      view.suggestions = [];
      view.suggestionsOpen = false;
      void render();
    }
  };

  app.addEventListener("click", async (event) => {
    const accountAction = event.target.closest("button[data-account-action]");
    if (accountAction) {
      event.preventDefault();
      clearAccountMessage();
      const dialog = app.querySelector("#password-dialog");
      if (accountAction.dataset.accountAction === "password") {
        app.querySelector(".account-menu")?.removeAttribute("open");
        dialog?.querySelector("form")?.reset();
        const error = dialog?.querySelector(".form-error");
        if (error) error.textContent = "";
        dialog?.showModal();
      } else if (accountAction.dataset.accountAction === "cancel-password") {
        dialog?.close();
      } else if (accountAction.dataset.accountAction === "logout") {
        accountAction.disabled = true;
        try {
          await post("/account/logout");
          window.location.assign(`${ROOT}/login`);
        } catch (error) {
          accountAction.disabled = false;
          setAccountMessage(error.message, true);
        }
      }
      return;
    }
    const dismiss = event.target.closest("button[data-dismiss-action-message]");
    if (dismiss) {
      event.preventDefault();
      clearActionMessage();
      return;
    }
    const nav = event.target.closest("a[data-nav]");
    if (nav) {
      // An unconditional preventDefault() would take Cmd-click and Shift-click
      // away from every link in the app.
      if (modifiedClick(event)) return;
      event.preventDefault();
      navigate(nav.getAttribute("href"));
      return;
    }
    const nameOption = event.target.closest("button[data-job-name]");
    if (nameOption) {
      event.preventDefault();
      const { view, key } = entries[nameOption.dataset.entryKind];
      chooseJobName(view, key, nameOption.dataset.jobName);
      return;
    }
    const tab = event.target.closest("button[data-status]");
    if (tab) {
      event.preventDefault();
      const { view: entryView, key: entryKey } = entries[tab.dataset.entryKind];
      const { status } = tab.dataset;
      if (entryView.statuses.has(status)) {
        entryView.statuses.delete(status);
      } else {
        entryView.statuses.add(status);
      }
      resetCursor(entryView);
      resetTableScroll(entryKey);
      // Emptied rather than re-asked: the names answered the old filter, and the
      // arrows fall back to this array whenever no list is on screen. Re-asking
      // would pop the list open unbidden. The next keystroke asks again.
      entryView.suggestions = [];
      entryView.suggestionIndex = -1;
      void render();
      return;
    }
    const kindTab = event.target.closest("button[data-kind]");
    if (kindTab) {
      event.preventDefault();
      entryKind = kindTab.dataset.kind;
      void render();
      return;
    }
    const rowNav = event.target.closest("tr[data-row-nav]");
    if (rowNav) {
      if (modifiedClick(event)) return;
      navigate(rowNav.dataset.rowNav);
      return;
    }
    const pagerButton = event.target.closest("button[data-page]");
    if (pagerButton && !pagerButton.disabled) {
      event.preventDefault();
      const name = pagerButton.dataset.pager;
      const direction = Number(pagerButton.dataset.page);
      if (name === "queues") {
        queuesView.offset = Math.max(0, queuesView.offset + direction * queuesView.limit);
      } else {
        const view = { workers: workersView, jobs: entries.job.view, crons: entries.cron.view }[name];
        if (!view) return;
        if (direction > 0 && view.nextCursor) {
          view.history.push({ cursor: view.cursor, start: view.start });
          view.cursor = view.nextCursor;
          view.start += view.pageCount;
          // render() is async and is what refreshes these; clearing now stops a
          // second click from re-using the stale cursor and paging twice.
          view.nextCursor = null;
          view.pageCount = 0;
        } else if (direction < 0 && view.history.length) {
          const previous = view.history.pop();
          view.cursor = previous.cursor;
          view.start = previous.start;
          view.nextCursor = null;
          view.pageCount = 0;
        }
      }
      resetTableScroll(pagerButton.dataset.pager);
      void render();
      return;
    }
    const action = event.target.closest("button[data-action]");
    // actionInFlight, not action.disabled: `disabled` lives on the node and is
    // not in the markup, so a repaint during the POST installs a fresh enabled
    // button. Running jobs repaint on every heartbeat, so this window is normal.
    if (action && !action.disabled && !actionInFlight) {
      event.preventDefault();
      // Navigation changes the URL before its response replaces the previous job's buttons.
      if (location.pathname !== renderedPath) return;
      const { queue, id } = route();
      if (!queue || !id) return;
      clearActionMessage();
      actionInFlight = true;
      action.disabled = true;
      let navigated = false;
      let applied = false;
      // Every message below describes *this* job, and jobView is the only view
      // with a slot to paint one into, so a report must not land after leaving.
      const issuedPath = location.pathname;
      const stillHere = () => location.pathname === issuedPath;
      try {
        const result = await post(
          `/queues/${encodeURIComponent(queue)}/jobs/${id}/${action.dataset.action}`,
        );
        // stillHere() for the reason every message below carries it: this is a
        // report about the job the operator was looking at, and one that lands
        // after they have left would throw away wherever they went for a job
        // page they never asked for. Falling through instead treats the retry
        // as applied, which repaints whatever page they are on now.
        if (action.dataset.action === "retry" && result.job_id && stillHere()) {
          navigated = true;
          navigate(`${ROOT}/queues/${encodeURIComponent(queue)}/jobs/${result.job_id}`);
          return;
        }
        // `false` is the server declining, not failing: the retry statement
        // guards on `retried_at IS NULL` and abort only reaches a live job. The
        // page it was clicked from is up to 5s stale, so say which it was.
        if (result.retried === false) {
          if (stillHere()) setActionMessage(
            "This job was not retried: it has already been retried, or its status changed.",
            true,
          );
        } else if (result.aborted === false) {
          if (stillHere()) setActionMessage(
            "This job was not aborted: it had already finished or been aborted.",
            true,
          );
        } else {
          // Drop the cache: the repaint below reads an API that may not have
          // caught up, and identical markup is skipped — which would leave the
          // button this handler disabled dead until some other field changed.
          applied = true;
          lastMarkup = null;
        }
        void render();
      } catch (error) {
        // Inline, not errorView: replacing #app destroys the job detail, and the
        // next poll wipes the error before it can be read.
        if (stillHere()) setActionMessage(error.message || String(error), true);
      } finally {
        // Re-armed only on endings that leave the job as the operator found it.
        // navigate() merely *starts* the repaint, so this node is still on screen
        // while route() already answers with the new job; an enabled Retry here
        // posts against the job just created. Applied aborts are drawn disabled.
        actionInFlight = false;
        if (!navigated && !applied) action.disabled = false;
        // A repaint during the POST replaced this node, so the line above went to
        // a detached button while its successor stays force-disabled. That state
        // is not in lastMarkup, so force a paint or both buttons stay dead.
        if (!navigated && !action.isConnected) {
          lastMarkup = null;
          void render();
        }
      }
    }
  });

  app.addEventListener("submit", async (event) => {
    if (event.target.matches("#password-form")) {
      event.preventDefault();
      const form = event.target;
      const error = form.querySelector(".form-error");
      const currentPassword = form.elements.current_password.value;
      const newPassword = form.elements.new_password.value;
      const confirmPassword = form.querySelector('[name="confirm_password"]');
      if (newPassword !== confirmPassword.value) {
        error.textContent = "New passwords do not match.";
        return;
      }
      const submit = form.querySelector('button[type="submit"]');
      submit.disabled = true;
      error.textContent = "";
      try {
        await post("/account/password", {
          current_password: currentPassword,
          new_password: newPassword,
        });
        form.reset();
        app.querySelector("#password-dialog")?.close();
        setAccountMessage("Password changed", false);
      } catch (submitError) {
        error.textContent = submitError.message;
      } finally {
        submit.disabled = false;
      }
      return;
    }
    if (!event.target.matches(".search-filter")) return;
    event.preventDefault();
    if (!event.target.matches(".job-name-search")) return;
    const { view, key } = entries[event.target.dataset.entryKind];
    // Read back out of the painted list, not view.suggestions — see
    // suggestionOptions. Escape and outside clicks remove the list entirely, so
    // there is nothing to read and the typed query stands.
    const selected = suggestionOptions()[view.suggestionIndex]?.dataset.jobName;
    chooseJobName(view, key, selected ?? view.query.trim());
  });

  app.addEventListener("keydown", (event) => {
    if (event.target.matches("#job-name-filter, #cron-name-filter")) {
      const view = entries[event.target.id === "cron-name-filter" ? "cron" : "job"].view;
      // Clamped to what is painted; state stands in only when nothing is, which
      // is how the arrows reopen a list Escape or an outside click removed.
      const options = suggestionOptions();
      const count = options.length || view.suggestions.length;
      if (["ArrowDown", "ArrowUp"].includes(event.key) && count) {
        event.preventDefault();
        const delta = event.key === "ArrowDown" ? 1 : -1;
        view.suggestionIndex = Math.max(
          -1,
          Math.min(count - 1, view.suggestionIndex + delta),
        );
        view.suggestionsOpen = true;
        // Repainting a removed list is a no-op, so with nothing on screen the
        // highlight has to come from a render instead.
        if (options.length) {
          paintSuggestionSelection(view);
        } else {
          void render();
        }
        return;
      }
      if (event.key === "Escape") {
        // Cancelled ahead of the open check rather than inside it:
        // suggestionsOpen only goes true when a response lands, so the first
        // request for a prefix sits on the wire with nothing yet on screen, and
        // an Escape that skipped the cancel let that response open the very
        // list it dismissed. preventDefault stays behind the check — with no
        // list to close, Escape belongs to the search input's own clear.
        cancelSuggestionFetch(view);
        if (view.suggestionsOpen) {
          event.preventDefault();
          view.suggestionsOpen = false;
          // The highlight goes with the list: the arrows never write it into
          // the input, so a surviving index is invisible and Enter would
          // commit it.
          view.suggestionIndex = -1;
          removeSuggestionList();
        }
        return;
      }
    }
    const rowNav = event.target.closest("tr[data-row-nav]");
    if (rowNav && event.target === rowNav && ["Enter", " "].includes(event.key)) {
      event.preventDefault();
      navigate(rowNav.dataset.rowNav);
    }
  });

  app.addEventListener("input", (event) => {
    if (event.target.id === "queue-name-filter") {
      queuesView.name = event.target.value;
      queuesView.offset = 0;
      resetTableScroll("queues");
      clearTimeout(queueSearchTimer);
      queueSearchTimer = setTimeout(render, 250);
    }
    const kind = { "job-name-filter": "job", "cron-name-filter": "cron" }[event.target.id];
    if (kind) {
      const { view, key } = entries[kind];
      view.query = event.target.value;
      if (view.query !== view.name) view.name = "";
      // The whole previous prefix is invalidated, not just the index into it, or
      // the arrows reopen names unrelated to the input. suggestionsOpen stays as
      // it is: the user dismissed nothing, and queueView renders the list only
      // while there are names to put in it.
      view.suggestions = [];
      view.suggestionIndex = -1;
      paintSuggestionSelection(view);
      resetCursor(view);
      resetTableScroll(key);
      clearTimeout(suggestionTimer);
      suggestionTimer = setTimeout(() => loadJobNameSuggestions(view, kind), 250);
    }
  });

  document.addEventListener("click", (event) => {
    const accountMenu = app.querySelector(".account-menu[open]");
    if (accountMenu && !event.target.closest(".account-menu")) {
      accountMenu.removeAttribute("open");
    }
    if (event.target.closest(".job-name-search")) return;
    for (const { view } of Object.values(entries)) {
      cancelSuggestionFetch(view);
      view.suggestionsOpen = false;
      view.suggestionIndex = -1;
    }
    removeSuggestionList();
  });

  // `close` does not bubble, hence the capture phase.
  app.addEventListener(
    "close",
    (event) => {
      if (event.target.matches?.(".account-dialog") && renderRequested) {
        renderRequested = false;
        void render();
      }
    },
    true,
  );

  // Back and Forward leave the page just as navigate() does, with no click for
  // the document handler to catch.
  window.addEventListener("popstate", () => {
    for (const { view } of Object.values(entries)) cancelSuggestionFetch(view);
    clearActionMessage();
    void render();
  });
  setInterval(() => {
    if (document.visibilityState === "visible") void render();
  }, REFRESH_MS);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") void render();
  });
  void render();
};

if (typeof document !== "undefined") startDashboard();
