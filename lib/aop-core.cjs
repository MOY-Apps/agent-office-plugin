// The emitter core: everything an AOP emitter needs that is neither harness-specific
// nor transport-specific.
//
// Forked from The Roving Office (atlassian-labs/roving-office, bin/lib/aop-core.cjs,
// Apache-2.0; see NOTICE). Three things live here, as upstream:
//
//   1. **Where the office is** (`resolveEndpoint`, `isLoopback`) — spec §5.5.
//   2. **Which office a directory belongs to** (`deriveProject`) — spec §3.4.1.
//   3. **How much may be said, and how short** (`resolveRedaction`, `clamp`,
//      `tidyPath`, CAPS) — spec §10.
//
// What this fork changes, and why (details in NOTICE):
//
//   - The config directory is `~/.agent-office/` (`AGENT_OFFICE_HOME` overrides it),
//     and the endpoint is resolved from `AGENT_OFFICE_URL`/`AGENT_OFFICE_TOKEN`
//     first, then the legacy `AOP_URL`/`AOP_TOKEN`, then `~/.agent-office/endpoint.json`,
//     then the legacy `~/.roving-office/endpoint.json` — so a machine that was set up
//     for the upstream plugin keeps working without being touched.
//   - **No gzip, ever.** The receiver is a Forge web trigger, and a gzipped body does
//     not survive the platform (it is decoded as UTF-8 on the way in and cannot be
//     recovered). `post` therefore sends plain bytes whatever the size, and splits an
//     NDJSON body into several POSTs under `BATCH_MAX_BYTES` instead.

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const https = require('https');
const { execFileSync } = require('child_process');

const PKG = require('../package.json');

const HOME = os.homedir();
/** Our own config dir. `AGENT_OFFICE_HOME` exists for tests and for shared machines. */
const DIR = process.env.AGENT_OFFICE_HOME || path.join(HOME, '.agent-office');
/** Where the upstream plugin keeps its files; read as a fallback, never written. */
const LEGACY_DIR = path.join(HOME, '.roving-office');
const ENDPOINT_FILE = path.join(DIR, 'endpoint.json');
const LEGACY_ENDPOINT_FILE = path.join(LEGACY_DIR, 'endpoint.json');
const PROJECT_CACHE = path.join(DIR, 'project-cache.json');
const ALIAS_FILE = path.join(DIR, 'projects.json');     // optional repo-slug → office-id map
const SETTINGS_FILE = path.join(DIR, 'settings.json');  // redaction, without an env var

const GIT_MS = 400;                        // per git invocation, and only on a cache miss
const PROJECT_TTL_MS = 60 * 60 * 1000;
const PROJECT_CACHE_MAX = 64;
const PROJECT_CACHE_V = 2;

/** The wire version this emitter speaks, spec §11. Minor bumps are additive. */
const AOP_VERSION = '0.2';

/**
 * The most an HTTP body may carry. The receiver refuses 256 KiB; the platform in
 * front of it has its own ideas about large bodies; 200 KB leaves room for both.
 * A batch larger than this goes out as several POSTs (see `post`).
 */
const BATCH_MAX_BYTES = 200 * 1000;

/** Truncation caps, spec §10. */
const CAPS = {
  title: 80, summary: 200, message: 200, target: 200, prompt: 2000, label: 60, color: 40,
  step_title: 80,
  origin_name: 80, origin_id: 80, origin_schedule: 40, origin_detail: 200,
  avatar: 512,
};

// --- small helpers ---------------------------------------------------------

function readJson(file, fallback = null) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}

/** Write via temp + rename so a concurrent reader never sees half a file. */
function writeJsonAtomic(file, value) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.tmp-${process.pid}`;
    fs.writeFileSync(tmp, `${JSON.stringify(value)}\n`, { mode: 0o600 });
    fs.renameSync(tmp, file);
  } catch { /* state is a nicety, never a requirement */ }
}

/** Collapse whitespace, cap length, and mark the cut so nothing looks complete. */
function clamp(value, max) {
  if (value == null) return undefined;
  const flat = String(value).replace(/\s+/g, ' ').trim();
  if (!flat) return undefined;
  return flat.length <= max ? flat : `${flat.slice(0, max - 1)}…`;
}

/**
 * Make a path presentable and non-identifying: relative to the session's cwd when
 * it sits underneath it, `~`-prefixed otherwise (spec §10).
 */
function tidyPath(value, cwd) {
  if (!value) return undefined;
  let out = String(value);
  if (cwd && out.startsWith(cwd)) out = out.slice(cwd.length).replace(/^\/+/, '') || '.';
  else if (out.startsWith(HOME)) out = `~${out.slice(HOME.length)}`;
  return clamp(out, CAPS.target);
}

/** First non-empty string among several candidate keys — harness payloads vary. */
function firstString(...values) {
  for (const v of values) {
    if (typeof v === 'string' && v.trim()) return v.trim();
    if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  }
  return undefined;
}

function nowIso() { return new Date().toISOString(); }

/** RFC 3339 in UTC with milliseconds, from whatever the harness gave us. */
function toIso(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return new Date(value > 1e12 ? value : value * 1000).toISOString();
  }
  if (typeof value === 'string') {
    const t = Date.parse(value);
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return nowIso();
}

/**
 * The bag of helpers a mapper is handed — the whole of what `map()` may assume
 * about its host beyond the payload itself. Frozen: a mapper reads it, nothing writes.
 */
const MAPPER_HELPERS = Object.freeze({ clamp, tidyPath, firstString, toIso, CAPS });

// --- project derivation (spec §3.4.1) --------------------------------------

function git(cwd, args) {
  try {
    return execFileSync('git', ['-C', cwd, ...args], {
      encoding: 'utf8', timeout: GIT_MS, stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch { return ''; }
}

/**
 * `git@host:owner/repo.git` and `https://host/owner/repo` → `host/owner/repo`.
 * The userinfo cut is at the **last** `@` so a credential in the remote never
 * reaches the slug; a path that cannot be a host/owner/repo becomes null.
 */
function canonicalRemote(remote) {
  if (!remote) return null;
  let s = String(remote).trim().replace(/\.git$/, '');
  s = s.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '');
  const at = s.lastIndexOf('@');
  if (at !== -1) s = s.slice(at + 1);
  s = s.replace(':', '/').replace(/\/{2,}/g, '/');
  const parts = s.split('/').filter(Boolean);
  if (parts.length < 2) return null;
  const slug = parts.join('/');
  return /^[A-Za-z0-9._~+%-]+(?:\/[A-Za-z0-9._~+%-]+)+$/.test(slug) ? slug : null;
}

/** The remote as an office may see it: a canonical slug, a tidied directory, or nothing. */
function wireRemote(remote) {
  if (!remote) return undefined;
  const s = String(remote).trim();
  const local = s.replace(/^file:\/\//i, '');
  if (/^(?:\/|~\/|\.{1,2}\/)/.test(local)) return tidyPath(local.replace(/\.git$/, ''), null);
  return canonicalRemote(s) ?? undefined;
}

/** `session` as an office may see it: `cwd` with `$HOME` collapsed to `~`, spec §10. */
function wireSession(session) {
  const cwd = session?.cwd;
  if (typeof cwd !== 'string' || !cwd) return session;
  const tidy = tidyPath(cwd, null);
  return tidy && tidy !== cwd ? { ...session, cwd: tidy } : session;
}

/**
 * Derive `project` for a cwd, cached because hooks fire many times a second and
 * shelling out to git on each one is exactly the latency the spec warns about.
 */
function deriveProject(cwd) {
  if (!cwd) return undefined;
  const cache = readJson(PROJECT_CACHE, {}) ?? {};
  const hit = cache[cwd];
  const fresh = hit && hit.v === PROJECT_CACHE_V && Date.now() - (hit.at ?? 0) < PROJECT_TTL_MS;
  let project = fresh ? hit.project : null;

  if (!project) {
    const remote = git(cwd, ['remote', 'get-url', 'origin']);
    const slug = canonicalRemote(remote);
    const top = git(cwd, ['rev-parse', '--show-toplevel']);
    const id = slug ? slug.split('/').pop() : path.basename(top || cwd);
    project = { id, name: id };
    const branch = git(cwd, ['rev-parse', '--abbrev-ref', 'HEAD']);
    const wire = wireRemote(remote);
    if (wire || branch) {
      project.repo = {};
      if (wire) project.repo.remote = wire;
      if (branch && branch !== 'HEAD') project.repo.branch = branch;
    }
    cache[cwd] = { at: Date.now(), v: PROJECT_CACHE_V, project };
    const keys = Object.keys(cache);
    if (keys.length > PROJECT_CACHE_MAX) {
      for (const k of keys.slice(0, keys.length - PROJECT_CACHE_MAX)) delete cache[k];
    }
    writeJsonAtomic(PROJECT_CACHE, cache);
  }

  const aliasFile = readJson(ALIAS_FILE, null);
  const aliases = aliasFile?.aliases ?? aliasFile ?? {};
  const slug = canonicalRemote(project.repo?.remote);
  const alias = aliases[slug] ?? aliases[project.id];
  if (typeof alias === 'string' && alias) project = { ...project, id: alias };

  // §3.4.3: one env var collapses every repo into a single office.
  const scene = process.env.AGENT_OFFICE_SCENE ?? process.env.ROVING_OFFICE_SCENE;
  return scene ? { ...project, scene } : project;
}

// --- endpoint (spec §5.5) --------------------------------------------------

/** Loopback or not, which is the only thing that changes an emitter's timing budget. */
function isLoopback(url) {
  try {
    const { hostname } = new URL(url);
    return /^127\.\d+\.\d+\.\d+$/.test(hostname)
      || hostname === '::1' || hostname === '[::1]' || hostname === 'localhost';
  } catch {
    return false;
  }
}

/**
 * Which office these events belong to, and how to prove we may write to it.
 *
 * Sources, highest precedence first:
 *
 * 0. An explicit `override` (a long-lived bridge configured by its host).
 * 1. `AGENT_OFFICE_URL` / `AGENT_OFFICE_TOKEN` in the environment.
 * 2. `AOP_URL` / `AOP_TOKEN` — the upstream names, kept so an existing shell works.
 * 3. `~/.agent-office/endpoint.json`, written by `agent-office connect`.
 * 4. `~/.roving-office/endpoint.json`, written by the upstream plugin or a local
 *    Roving Office. Read, never written: it is somebody else's file.
 *
 * `remote` is derived rather than trusted from the file.
 *
 * @param {{url?: string, token?: string}|null} override
 * @param {{env?: object}} [options] the environment to read, for tests
 */
function resolveEndpoint(override = null, { env = process.env } = {}) {
  let endpoint = null;
  if (override?.url) endpoint = { url: override.url, token: override.token ?? null, source: 'config' };
  else if (env.AGENT_OFFICE_URL) endpoint = { url: env.AGENT_OFFICE_URL, token: env.AGENT_OFFICE_TOKEN ?? null, source: 'env' };
  else if (env.AOP_URL) endpoint = { url: env.AOP_URL, token: env.AOP_TOKEN ?? null, source: 'env:legacy' };
  else {
    const own = readJson(ENDPOINT_FILE, null);
    if (own?.url) endpoint = { ...own, source: 'file' };
    else {
      const legacy = readJson(LEGACY_ENDPOINT_FILE, null);
      if (legacy?.url) endpoint = { ...legacy, source: 'file:legacy' };
    }
  }
  if (!endpoint?.url) return null;
  endpoint.remote = !isLoopback(endpoint.url);
  return endpoint;
}

// --- delivery --------------------------------------------------------------

/** Byte length of an NDJSON line plus its newline. */
function lineBytes(line) { return Buffer.byteLength(line, 'utf8') + 1; }

/**
 * Cut a list of NDJSON lines into batches that each fit under `maxBytes` on the
 * wire. Order is kept. A single line that is itself over the limit cannot be
 * delivered at all and is returned separately, so the caller can drop it rather
 * than let it block the spool forever.
 *
 * @returns {{batches: string[][], oversize: string[]}}
 */
function splitBatches(lines, maxBytes = BATCH_MAX_BYTES) {
  const batches = [];
  const oversize = [];
  let current = [];
  let size = 0;
  for (const line of lines) {
    const n = lineBytes(line);
    if (n > maxBytes) { oversize.push(line); continue; }
    if (size + n > maxBytes && current.length) { batches.push(current); current = []; size = 0; }
    current.push(line);
    size += n;
  }
  if (current.length) batches.push(current);
  return { batches, oversize };
}

/**
 * The headers every POST carries. Exported so a test can prove there is no
 * `Content-Encoding` among them whatever the body size.
 */
function requestHeaders(endpoint, data) {
  return {
    'Content-Type': 'application/x-ndjson',
    'Content-Length': data.length,
    // `X-Roving-Office-Token` is the AOP v0 header; the receiver also accepts
    // `Authorization: Bearer`, but hosting layers claim that header for themselves,
    // so the protocol header is the one that survives the trip.
    'X-Roving-Office-Token': endpoint.token ?? '',
    'User-Agent': `agent-office/${PKG.version} aop/${AOP_VERSION} (+${PKG.homepage})`,
    Accept: 'application/json',
  };
}

/** One POST of plain bytes. Resolves `{ok, status, reason, body}`, never throws. */
function postOnce(endpoint, body, { connectMs, requestMs }) {
  return new Promise((resolve) => {
    let url;
    try { url = new URL(endpoint.url); } catch { return resolve({ ok: false, status: null, reason: `malformed url: ${endpoint.url}`, body: null }); }
    const data = Buffer.from(body, 'utf8');
    const headers = requestHeaders(endpoint, data);

    // `URL.hostname` keeps the brackets on an IPv6 literal; `http.request` wants it bare.
    const hostname = url.hostname.replace(/^\[|\]$/g, '');
    const lib = url.protocol === 'https:' ? https : http;
    let settled = false;
    const done = (value) => { if (!settled) { settled = true; resolve(value); } };

    const req = lib.request({
      protocol: url.protocol,
      hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: 'POST',
      headers,
    }, (res) => {
      const ok = res.statusCode >= 200 && res.statusCode < 300;
      if (ok) {
        res.resume();
        return done({ ok: true, status: res.statusCode, reason: null, body: null });
      }
      let text = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { if (text.length < 300) text += c; });
      res.on('end', () => done({
        ok: false,
        status: res.statusCode,
        reason: `HTTP ${res.statusCode}${res.statusMessage ? ` ${res.statusMessage}` : ''}`,
        body: text.slice(0, 300).replace(/\s+/g, ' ').trim() || null,
      }));
      res.on('error', () => done({ ok: false, status: res.statusCode, reason: `HTTP ${res.statusCode}`, body: null }));
    });
    req.setTimeout(connectMs, () => { req.destroy(); done({ ok: false, status: null, reason: `no response within ${connectMs}ms`, body: null }); });
    const overall = setTimeout(() => { req.destroy(); done({ ok: false, status: null, reason: `gave up after ${requestMs}ms`, body: null }); }, requestMs);
    overall.unref?.();
    req.on('error', (err) => done({ ok: false, status: null, reason: err?.code ?? err?.message ?? 'connection failed', body: null }));
    req.on('close', () => clearTimeout(overall));
    req.end(data);
  });
}

/**
 * POST an NDJSON batch. Resolves `{ok, status, reason, body, posts, dropped}` and
 * never throws — spec §5.1 says an emitter treats every failure identically.
 *
 * Plain bytes only: there is no gzip branch in this fork, and a `gzipOver` in the
 * budget is accepted for call-site compatibility and ignored. A body larger than
 * `maxBytes` is cut on line boundaries into several sequential POSTs; the first
 * failure stops the run and is what is reported, so the caller re-spools the lot.
 *
 * @param {{url: string, token?: string|null}} endpoint
 * @param {string} body NDJSON
 * @param {{connectMs: number, requestMs: number, maxBytes?: number}} budget
 */
async function post(endpoint, body, budget) {
  const { connectMs, requestMs, maxBytes = BATCH_MAX_BYTES } = budget;
  const lines = String(body ?? '').split('\n').filter(Boolean);
  if (!lines.length) {
    // An empty probe takes the real ingest path with nothing in it.
    const res = await postOnce(endpoint, '', { connectMs, requestMs });
    return { ...res, posts: 1, dropped: 0 };
  }
  const { batches, oversize } = splitBatches(lines, maxBytes);
  let last = { ok: true, status: null, reason: null, body: null };
  let posts = 0;
  for (const batch of batches) {
    last = await postOnce(endpoint, batch.join('\n'), { connectMs, requestMs });
    posts += 1;
    if (!last.ok) break;
  }
  return { ...last, posts, dropped: oversize.length };
}

// --- who is at the keyboard (spec §3.2, session.actor) -----------------------

/**
 * The OS user's full name, or their login when the system has none: `id -F` on macOS,
 * the GECOS field of /etc/passwd on Linux and the like, the login elsewhere. Memoised,
 * since the answer cannot change under a running process.
 */
let osActorMemo;
function osActor({ platform = process.platform, run = execFileSync, passwd = '/etc/passwd', user = os.userInfo() } = {}) {
  let full = null;
  try {
    if (platform === 'darwin') {
      full = String(run('id', ['-F'], { encoding: 'utf8', timeout: 300, stdio: ['ignore', 'pipe', 'ignore'] })).trim();
    } else if (platform !== 'win32') {
      const line = fs.readFileSync(passwd, 'utf8').split('\n').find((l) => l.split(':')[2] === String(user.uid));
      full = line ? line.split(':')[4]?.split(',')[0]?.trim() : null;
    }
  } catch { full = null; }
  return clamp(full, CAPS.label) ?? clamp(user.username, CAPS.label) ?? null;
}

/**
 * The name the office shows for this machine's sessions, `session.actor` on every
 * envelope. Highest precedence first:
 *
 *   1. `AGENT_OFFICE_ACTOR` in the environment (a shell, a CI job, a shared box);
 *   2. `actor` in `~/.agent-office/endpoint.json`, written by `connect --name`;
 *   3. the OS user's full name, or their login.
 *
 * `stored` stands in for the file's value when given (connect, before it writes);
 * `fallback` for the OS lookup (tests).
 *
 * @returns {{actor: string|null, source: 'env'|'file'|'os'}}
 */
function resolveActor({ env = process.env, file = ENDPOINT_FILE, stored, fallback = null } = {}) {
  const fromEnv = clamp(env.AGENT_OFFICE_ACTOR, CAPS.label);
  if (fromEnv) return { actor: fromEnv, source: 'env' };
  const fromFile = clamp(stored !== undefined ? stored : readJson(file, null)?.actor, CAPS.label);
  if (fromFile) return { actor: fromFile, source: 'file' };
  if (fallback) return { actor: fallback(), source: 'os' };
  if (osActorMemo === undefined) osActorMemo = osActor();
  return { actor: osActorMemo, source: 'os' };
}

// --- redaction (spec §10) --------------------------------------------------

/**
 * Env over `settings.json` over the safe default; `full` always needs a second opt-in.
 * The upstream variable names are read as a fallback so an existing setup keeps its mode.
 */
function resolveRedaction({ env = process.env } = {}) {
  const file = readJson(SETTINGS_FILE, null) ?? {};
  const mode = String(env.AGENT_OFFICE_REDACTION ?? env.ROVING_OFFICE_REDACTION ?? file.redaction ?? '').toLowerCase();
  const promptsAllowed = env.AGENT_OFFICE_INCLUDE_PROMPTS === '1' || env.ROVING_OFFICE_INCLUDE_PROMPTS === '1' || file.includePrompts === true;
  if (mode === 'full') return promptsAllowed ? 'full' : 'summary';
  if (mode === 'summary') return 'summary';
  return 'metadata';
}

/**
 * Answers that will never succeed for this body, however often it is sent: a
 * 4xx other than 401 (the token may be fixed), 408 and 429 (try later). The
 * receiver answers 400 for a body it cannot read and 413 for one too large;
 * retrying either would keep the batch at the head of the spool forever and
 * stop the machine from sending anything at all.
 */
function isFinal(status) {
  return Number.isInteger(status) && status >= 400 && status < 500 && ![401, 408, 429].includes(status);
}

/**
 * Send spooled lines as POSTs under `maxBytes`, deciding per batch what to
 * keep:
 *
 * - 2xx: delivered;
 * - 413: cut in two and each half sent once more; a half that is refused
 *   finally again is dropped;
 * - any other final answer (isFinal): dropped;
 * - 401, 408, 429, 5xx, no answer: stop here, and keep this batch and every
 *   one after it, in order, for the next attempt.
 *
 * Never throws. `done` is true when nothing is left to retry.
 *
 * @returns {Promise<{done: boolean, keep: string[], delivered: number, dropped: {lines: number, status: number|null, reason: string}[], posts: number, status: number|null, reason: string|null}>}
 */
async function deliver(endpoint, lines, budget) {
  const { connectMs, requestMs, maxBytes = BATCH_MAX_BYTES } = budget;
  const { batches, oversize } = splitBatches(lines, maxBytes);
  const dropped = [];
  if (oversize.length) dropped.push({ lines: oversize.length, status: null, reason: `line over ${maxBytes} bytes` });
  let delivered = 0;
  let posts = 0;
  let last = { ok: true, status: null, reason: null };
  const send = async (batch) => {
    posts += 1;
    last = await postOnce(endpoint, batch.join('\n'), { connectMs, requestMs });
    return last;
  };
  const retryLater = (rest) => ({ done: false, keep: rest, delivered, dropped, posts, status: last.status, reason: last.reason });
  for (let i = 0; i < batches.length; i += 1) {
    const batch = batches[i];
    const res = await send(batch);
    if (res.ok) { delivered += batch.length; continue; }
    if (res.status === 413 && batch.length > 1) {
      const half = Math.ceil(batch.length / 2);
      const parts = [batch.slice(0, half), batch.slice(half)];
      for (let j = 0; j < parts.length; j += 1) {
        const r = await send(parts[j]);
        if (r.ok) { delivered += parts[j].length; continue; }
        if (isFinal(r.status)) { dropped.push({ lines: parts[j].length, status: r.status, reason: r.reason }); continue; }
        return retryLater([...parts.slice(j).flat(), ...batches.slice(i + 1).flat()]);
      }
      continue;
    }
    if (isFinal(res.status)) { dropped.push({ lines: batch.length, status: res.status, reason: res.reason }); continue; }
    return retryLater(batches.slice(i).flat());
  }
  return { done: true, keep: [], delivered, dropped, posts, status: last.status, reason: last.reason };
}

module.exports = {
  HOME, DIR, LEGACY_DIR, ENDPOINT_FILE, LEGACY_ENDPOINT_FILE, PROJECT_CACHE, ALIAS_FILE, SETTINGS_FILE,
  AOP_VERSION, CAPS, BATCH_MAX_BYTES,
  readJson, writeJsonAtomic,
  clamp, tidyPath, firstString, nowIso, toIso,
  MAPPER_HELPERS,
  git, canonicalRemote, wireRemote, wireSession, deriveProject,
  isLoopback, resolveEndpoint,
  osActor, resolveActor,
  splitBatches, requestHeaders, postOnce, post, isFinal, deliver,
  resolveRedaction,
};
