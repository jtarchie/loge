// loge web UI: a thin client over the server's JSON API. It runs LogQL queries,
// renders results, and live-tails by polling /api/v1/query. Auth is the same
// shared API key the CLI uses, stored in localStorage and sent as a bearer token.

const KEY_STORE = "loge.apiKey";
const THEME_STORE = "loge.theme";
const MS_TO_NS = 1_000_000; // log timestamps are unix nanoseconds
const TAIL_INTERVAL_MS = 2000;
const TAIL_LIMIT = 500;
const MAX_ROWS = 5000;
// Re-fetch a small slack window each tail tick so a row whose timestamp lands
// right at the boundary (or arrives a touch late) is never skipped; duplicates
// are dropped by the (timestamp,line) dedupe below. Far larger than any float
// rounding at nanosecond scale.
const TAIL_SLACK_NS = 5 * MS_TO_NS;

const $ = (id) => document.getElementById(id);
const els = {};

let apiKey = localStorage.getItem(KEY_STORE) || "";
let tailing = false;
let tailTimer = null;
let seen = new Set();
let lastSeenTs = 0;

// ---- theme -----------------------------------------------------------------

function applyTheme(theme) {
  if (theme === "light" || theme === "dark") {
    document.documentElement.setAttribute("data-theme", theme);
  } else {
    document.documentElement.removeAttribute("data-theme"); // fall back to prefers-color-scheme
  }
}

function currentTheme() {
  return (
    document.documentElement.getAttribute("data-theme") ||
    (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light")
  );
}

function toggleTheme() {
  const next = currentTheme() === "dark" ? "light" : "dark";
  localStorage.setItem(THEME_STORE, next);
  applyTheme(next);
}

// ---- api -------------------------------------------------------------------

function authHeaders() {
  return apiKey ? { Authorization: "Bearer " + apiKey } : {};
}

async function api(path, options = {}) {
  const resp = await fetch(path, {
    ...options,
    headers: { ...(options.headers || {}), ...authHeaders() },
  });
  if (resp.status === 401) {
    // Key missing/invalid or revoked mid-session: drop back to the login gate.
    showLogin();
    throw new Error("unauthorized");
  }
  return resp;
}

// ---- query -----------------------------------------------------------------

// Build the POST body. `startNs` of 0 means unbounded (server treats 0 as such);
// nanosecond precision on a human time filter is irrelevant, so a plain JSON
// number is fine here.
function queryBody(startNs, limit) {
  return JSON.stringify({
    query: els.selector.value.trim(),
    start: startNs,
    end: 0,
    limit,
  });
}

async function runQuery(body) {
  const resp = await api("/api/v1/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  if (!resp.ok) {
    const text = (await resp.text()).trim();
    throw new Error(text || `server returned ${resp.status}`);
  }
  const payload = await resp.json();
  return payload.data || [];
}

function rangeStartNs() {
  const windowMs = Number(els.range.value);
  if (windowMs === 0) return 0; // "Any time"
  return (Date.now() - windowMs) * MS_TO_NS;
}

function clampLimit() {
  const n = parseInt(els.limit.value, 10);
  if (!Number.isFinite(n) || n < 1) return 100;
  return Math.min(n, 5000);
}

async function onSubmit(event) {
  event.preventDefault();
  stopTail();
  resetResults();
  setStatus("Running…");
  try {
    const entries = await runQuery(queryBody(rangeStartNs(), clampLimit()));
    entries.forEach((e) => remember(e));
    renderAppend(entries, false);
    setStatus(entries.length ? `${entries.length} result${entries.length === 1 ? "" : "s"}` : "No results");
  } catch (err) {
    if (err.message !== "unauthorized") setStatus(err.message, true);
  }
}

// ---- live tail -------------------------------------------------------------
//
// Poll /api/v1/query on an interval (setTimeout-chained so requests never
// overlap). Each tick fetches from `lastSeenTs - slack` (inclusive start), so
// boundary/late rows reappear and are deduped by (timestamp,line). New rows are
// prepended, keeping the list newest-first.

function entryKey(e) {
  return e.timestamp + " " + e.line;
}

function remember(e) {
  seen.add(entryKey(e));
  if (e.timestamp > lastSeenTs) lastSeenTs = e.timestamp;
}

async function tailTick() {
  if (!tailing) return;
  try {
    const start = lastSeenTs > 0 ? lastSeenTs - TAIL_SLACK_NS : 0;
    const entries = await runQuery(queryBody(start, TAIL_LIMIT));
    const fresh = entries.filter((e) => !seen.has(entryKey(e)));
    fresh.forEach((e) => remember(e));
    if (fresh.length) {
      fresh.sort((a, b) => (a.timestamp < b.timestamp ? -1 : 1)); // oldest first, so prepend keeps newest on top
      renderPrepend(fresh);
      setStatus(`Tailing… ${els.results.childElementCount} lines`);
    }
  } catch (err) {
    if (err.message === "unauthorized") return; // login gate already shown
    setStatus(err.message, true);
  }
  if (tailing) tailTimer = setTimeout(tailTick, TAIL_INTERVAL_MS);
}

function startTail() {
  tailing = true;
  els.tail.setAttribute("aria-pressed", "true");
  els.tail.textContent = "Stop";
  // Seed from the newest row already shown (or now), so we don't re-pull history.
  if (lastSeenTs === 0) lastSeenTs = Date.now() * MS_TO_NS;
  setStatus("Tailing…");
  tailTick();
}

function stopTail() {
  tailing = false;
  if (tailTimer) clearTimeout(tailTimer);
  tailTimer = null;
  els.tail.setAttribute("aria-pressed", "false");
  els.tail.textContent = "Tail";
}

function toggleTail() {
  if (tailing) {
    stopTail();
    setStatus("Stopped");
  } else {
    resetResults();
    startTail();
  }
}

// ---- rendering -------------------------------------------------------------

function setStatus(text, isError = false) {
  els.status.textContent = text;
  els.status.classList.toggle("error", isError);
}

function resetResults() {
  els.results.replaceChildren();
  seen = new Set();
  lastSeenTs = 0;
}

function fmtTime(ts) {
  // ms precision is plenty for display; never feed the raw ns to Date.
  const d = new Date(ts / MS_TO_NS);
  return Number.isNaN(d.getTime()) ? String(ts) : d.toISOString().replace("T", " ").replace("Z", "");
}

function rowFor(entry, fresh) {
  const li = document.createElement("li");
  if (fresh) li.className = "fresh";

  const ts = document.createElement("span");
  ts.className = "ts";
  ts.textContent = fmtTime(entry.timestamp);
  li.append(ts);

  const labels = entry.labels || {};
  const keys = Object.keys(labels).sort();
  if (keys.length) {
    const chips = document.createElement("span");
    chips.className = "chips";
    for (const k of keys) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = `${k}=${labels[k]}`;
      chips.append(chip);
    }
    li.append(chips);
  }

  const line = document.createElement("span");
  line.className = "line";
  line.textContent = entry.line;
  li.append(line);
  return li;
}

function trim() {
  while (els.results.childElementCount > MAX_ROWS) {
    els.results.lastElementChild.remove();
  }
}

// Append below (used for one-shot queries, which arrive newest-first already).
function renderAppend(entries, fresh) {
  const frag = document.createDocumentFragment();
  for (const e of entries) frag.append(rowFor(e, fresh));
  els.results.append(frag);
  trim();
}

// Prepend above (used for tail; `entries` must be oldest-first so the newest
// ends up on top after successive inserts).
function renderPrepend(entries) {
  for (const e of entries) {
    els.results.prepend(rowFor(e, true));
  }
  trim();
}

// ---- auth / boot -----------------------------------------------------------

function showLogin(message) {
  els.app.hidden = true;
  els.login.hidden = false;
  if (message) {
    els.loginError.textContent = message;
    els.loginError.hidden = false;
  }
  els.loginKey.focus();
}

function showApp(authRequired) {
  els.login.hidden = true;
  els.app.hidden = false;
  els.signOut.hidden = !authRequired;
  els.selector.focus();
}

// Probe /api/v1/auth: 200 => proceed (response says whether a key is required);
// 401 => show the login gate.
async function checkAuth() {
  try {
    const resp = await fetch("/api/v1/auth", { headers: authHeaders() });
    if (resp.status === 401) {
      showLogin();
      return;
    }
    const { required } = await resp.json();
    showApp(Boolean(required));
  } catch {
    setStatus("Cannot reach the server.", true);
  }
}

async function onLogin(event) {
  event.preventDefault();
  apiKey = els.loginKey.value.trim();
  els.loginError.hidden = true;
  const resp = await fetch("/api/v1/auth", { headers: authHeaders() });
  if (resp.ok) {
    localStorage.setItem(KEY_STORE, apiKey);
    const { required } = await resp.json();
    showApp(Boolean(required));
  } else {
    apiKey = "";
    els.loginError.textContent = "Invalid API key.";
    els.loginError.hidden = false;
  }
}

function signOut() {
  stopTail();
  apiKey = "";
  localStorage.removeItem(KEY_STORE);
  els.loginKey.value = "";
  showLogin();
}

function boot() {
  Object.assign(els, {
    themeToggle: $("theme-toggle"),
    login: $("login"),
    loginForm: $("login-form"),
    loginKey: $("login-key"),
    loginError: $("login-error"),
    app: $("app"),
    queryBar: $("query-bar"),
    selector: $("selector"),
    range: $("range"),
    limit: $("limit"),
    run: $("run"),
    tail: $("tail"),
    signOut: $("sign-out"),
    status: $("status"),
    results: $("results"),
  });

  applyTheme(localStorage.getItem(THEME_STORE));
  els.themeToggle.addEventListener("click", toggleTheme);
  els.queryBar.addEventListener("submit", onSubmit);
  els.tail.addEventListener("click", toggleTail);
  els.signOut.addEventListener("click", signOut);
  els.loginForm.addEventListener("submit", onLogin);

  checkAuth();
}

boot();
