// The hook entry: read one hook payload on stdin, map it to Agent Office Protocol
// events and get them to the receiver without the agent noticing.
//
//   agent-office send <harness> [event-name] [--chain "<command>"]
//   bin/aop-send.cjs  <harness> [event-name]            (the same thing, one file)
//
// Forked from The Roving Office's bin/aop-send.cjs (Apache-2.0; see NOTICE). The
// design rules are upstream's and they are kept to the letter:
//
//   1. ALWAYS exit 0. Rovo CLI disables a hook that exits non-zero, and no harness
//      should ever lose its integration because an office was down.
//   2. Never make the agent wait. Hard watchdog at WATCHDOG_MS; a remote POST never
//      happens in the hook's process at all — it is spooled and handed to a detached
//      flusher.
//   3. Do nothing at all when no office is configured.
//   4. Redact at the emitter (spec §10).
//
// What this fork changes: the transport never gzips and cuts a batch into POSTs under
// 200 KB (both in lib/aop-core.cjs), and the endpoint comes from `~/.agent-office/`
// or `AGENT_OFFICE_URL`, with the upstream locations as fallbacks.

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');

const core = require('./aop-core.cjs');
const {
  AOP_VERSION, DIR, SETTINGS_FILE, ALIAS_FILE,
  readJson, writeJsonAtomic,
  firstString, nowIso, toIso,
  MAPPER_HELPERS,
  deriveProject, resolveEndpoint, wireSession,
  resolveRedaction, resolveActor,
} = core;

// --- budgets ---------------------------------------------------------------

const STDIN_MS = 250;        // a hook that sends nothing must not hang us
const CONNECT_MS = 500;      // spec §5.6, loopback
const REQUEST_MS = 1000;
const WATCHDOG_MS = 2500;    // absolute ceiling on a hook process

// The flusher's budgets. It is detached, so nobody is waiting on these. The request
// budget is generous on purpose: a POST the office did process but answered after
// we gave up is sent again, and that is a duplicate (APPS-401).
const REMOTE_CONNECT_MS = 2000;
const REMOTE_REQUEST_MS = 8000;
const FLUSH_WATCHDOG_MS = 25000;

/** Hooks append here; only the lock holder renames it away. */
const SPOOL_FILE = path.join(DIR, 'spool.ndjson');
// an hour of a busy session through a receiver outage or a long backoff (one event is ~0.5 KB)
const SPOOL_MAX_LINES = 5000;

/**
 * What a flusher is sending, or could not send: `spool.sending.<owner>.ndjson`, one
 * per lock owner, so two flushers never work on one file. Always ahead of the
 * spool. Files it adopted from others are `spool.sending.<owner>~<rand>.ndjson`;
 * 0.1.0's shared `spool.sending.ndjson` is read as an orphan too.
 */
const SENDING_RE = /^spool\.sending(?:\.([0-9a-f]+)(?:~[0-9a-f]+)?)?\.ndjson$/;

/**
 * One flusher per home directory. Created with O_EXCL, holds {host, pid, owner, at};
 * its mtime is the heartbeat, refreshed every HEARTBEAT_MS while it is held (Node's
 * timers stop while a laptop sleeps, so a living holder can look dead by age).
 * Taken over only when the heartbeat is stale and:
 *   - same host, pid gone: stale past LOCK_STALE_MS (two machines can share a
 *     hostname, so a dead pid alone is not proof);
 *   - same host, pid alive: past LOCK_ABANDONED_MS only (pid reuse); a sleeping
 *     holder keeps its lock;
 *   - another host (a home shared over NFS; its pid means nothing here): past
 *     LOCK_FOREIGN_STALE_MS.
 * A holder that wakes up and finds the lock no longer its own stops at its next
 * check; at worst the batch it had in flight is sent twice, and the receiver
 * dedups by id.
 */
const LOCK_FILE = path.join(DIR, 'flush.lock');
const HEARTBEAT_MS = 1000;
const LOCK_STALE_MS = 10 * 1000;
const LOCK_FOREIGN_STALE_MS = 60 * 1000;
const LOCK_ABANDONED_MS = 10 * 60 * 1000;
/** Who we are in the lock. AGENT_OFFICE_HOST overrides it for machines that share a hostname. */
const HOST = process.env.AGENT_OFFICE_HOST || os.hostname() || 'unknown-host';

/**
 * Coalescing (APPS-401): a fresh flusher waits this long for more events before it
 * POSTs, so a burst of hooks costs one request. AGENT_OFFICE_FLUSH_MS, 0..5000.
 */
const FLUSH_MS_DEFAULT = 1000;
const FLUSH_MS_MAX = 5000;
const FLUSH_POLL_MS = 50;

/** Events someone may be waiting on: they end the window at once. */
const URGENT_TYPES = new Set(['session.end', 'permission.request', 'notification', 'turn.end']);

/** One line per batch the office refused for good; never event contents. */
const LOG_FILE = path.join(DIR, 'adapter.log');
const LOG_MAX_BYTES = 64 * 1024;

/**
 * After a failure worth retrying, wait 2 s, 4 s, 8 s … up to 5 min (±20 %)
 * before the next POST to that endpoint. Kept per URL, so `connect` to another
 * office starts afresh.
 */
const BACKOFF_FILE = path.join(DIR, 'backoff.json');
const BACKOFF_BASE_MS = 2000;
const BACKOFF_MAX_MS = 5 * 60 * 1000;
const SAMPLE_MAX = 50;

/** The file a detached flusher is started from: one process, one job. */
const SEND_BIN = path.join(__dirname, '..', 'bin', 'aop-send.cjs');

// --- small helpers ---------------------------------------------------------

/** Read stdin, but never block on a hook that sends nothing. */
function readStdin() {
  return new Promise((resolve) => {
    if (process.stdin.isTTY) return resolve('');
    let buf = '';
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(buf); } };
    const timer = setTimeout(finish, STDIN_MS);
    timer.unref?.();
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (c) => { buf += c; });
    process.stdin.on('end', () => { clearTimeout(timer); finish(); });
    process.stdin.on('error', () => { clearTimeout(timer); finish(); });
  });
}

// --- per-session adapter state --------------------------------------------

const STATE_TTL_MS = 30 * 60 * 1000;

function stateFile(harness) { return path.join(DIR, `state-${harness}.json`); }

function loadState(harness) {
  const state = readJson(stateFile(harness), null) ?? { sessions: {} };
  const cutoff = Date.now() - STATE_TTL_MS;
  for (const [id, s] of Object.entries(state.sessions)) {
    if ((s.lastSeen ?? 0) < cutoff) delete state.sessions[id];
  }
  return state;
}

function sessionState(state, id) {
  if (!state.sessions[id]) state.sessions[id] = { seq: 0, started: false, turnOpen: false, tools: {} };
  return state.sessions[id];
}

// --- spool -----------------------------------------------------------------

/**
 * Append only. The cap (SPOOL_MAX_LINES) is the flusher's job, under its lock: a
 * hook rewriting the spool could resurrect lines a flusher had just taken.
 */
function appendSpool(lines) {
  try {
    fs.mkdirSync(DIR, { recursive: true });
    fs.appendFileSync(SPOOL_FILE, `${lines.join('\n')}\n`, { mode: 0o600 });
  } catch { /* the office simply misses these */ }
}

function readText(file) { try { return fs.readFileSync(file, 'utf8'); } catch { return ''; } }
const splitLines = (text) => text.split('\n').filter(Boolean);

const rand = (n = 6) => crypto.randomBytes(n).toString('hex');

/** The sending file of one lock owner. */
function sendingFile(owner) { return path.join(DIR, `spool.sending.${owner}.ndjson`); }

/** Every sending file there is, oldest first: {file, owner} (owner null for 0.1.0's). */
function sendingFiles() {
  let names;
  try { names = fs.readdirSync(DIR); } catch { return []; }
  const out = [];
  for (const name of names) {
    const m = SENDING_RE.exec(name);
    if (!m) continue;
    const file = path.join(DIR, name);
    let mtime;
    try { mtime = fs.statSync(file).mtimeMs; } catch { continue; }
    out.push({ file, owner: m[1] ?? null, mtime });
  }
  return out.sort((a, b) => a.mtime - b.mtime || a.file.localeCompare(b.file)).map(({ file, owner }) => ({ file, owner }));
}

/** Everything not yet delivered, oldest first: what is being sent, then the spool. */
function pendingLines() {
  return [...sendingFiles().flatMap((f) => splitLines(readText(f.file))), ...splitLines(readText(SPOOL_FILE))];
}

function hasPending() {
  for (const f of [...sendingFiles().map((x) => x.file), SPOOL_FILE]) {
    try { if (fs.statSync(f).size > 0) return true; } catch { /* absent */ }
  }
  return false;
}

/**
 * Take over every sending file another owner left (a dead flusher's, or one that
 * lost the lock) by renaming it to a name of ours. A rename moves the file whole and
 * only one renamer can win it, so its lines are re-queued exactly once and never
 * copied. Call under the lock. Returns the number of lines adopted.
 */
function adoptOrphans(owner) {
  let adopted = 0;
  for (const f of sendingFiles()) {
    if (f.owner === owner) continue;
    const claim = path.join(DIR, `spool.sending.${owner}~${rand(4)}.ndjson`);
    try { fs.renameSync(f.file, claim); } catch { continue; }   // another adopter won it
    adopted += splitLines(readText(claim)).length;
  }
  return adopted;
}

function writeLinesAtomic(file, lines) {
  if (!lines.length) { try { fs.unlinkSync(file); } catch { /* gone */ } return; }
  const tmp = `${file}.${rand(4)}.tmp`;
  fs.writeFileSync(tmp, `${lines.join('\n')}\n`, { mode: 0o600 });
  fs.renameSync(tmp, file);
}

/**
 * Take exclusive ownership of what is currently spooled, by rename: atomic on
 * POSIX, so exactly one sender wins a given batch and nothing arriving in the
 * meantime is lost.
 */
function claimSpool() {
  const claim = `${SPOOL_FILE}.${rand(4)}.claim`;
  try {
    fs.renameSync(SPOOL_FILE, claim);
  } catch {
    return { lines: [], release: () => {} };
  }
  let lines = [];
  try { lines = fs.readFileSync(claim, 'utf8').split('\n').filter(Boolean); } catch { /* unreadable */ }
  return {
    lines,
    /** Give back what is still to be sent (an array of lines), or nothing. */
    release: (keep = []) => {
      if (keep.length) appendSpool(keep);
      try { fs.unlinkSync(claim); } catch { /* already gone */ }
    },
  };
}

// --- the flusher's lock -------------------------------------------------------

function pidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
}

function readLock() {
  try { const info = JSON.parse(fs.readFileSync(LOCK_FILE, 'utf8')); return info && typeof info === 'object' ? info : null; }
  catch { return null; }
}

/** The lock as it stands: null when there is none, else whether it is stale (see LOCK_FILE). */
function lockState(now = Date.now()) {
  let st;
  try { st = fs.statSync(LOCK_FILE); } catch { return null; }
  const info = readLock();
  const age = now - st.mtimeMs;
  let limit;
  // An empty or unreadable lock is a taker caught between create and write: only a long age retires it.
  if (!info || info.host !== HOST) limit = LOCK_FOREIGN_STALE_MS;
  else limit = pidAlive(info.pid) ? LOCK_ABANDONED_MS : LOCK_STALE_MS;
  return { info, stale: age > limit };
}

/** Is the lock still `owner`'s? If so, refresh its heartbeat. */
function heartbeat(owner) {
  if (readLock()?.owner !== owner) return false;
  try { const now = new Date(); fs.utimesSync(LOCK_FILE, now, now); } catch { /* the next beat tries again */ }
  return true;
}

/** Is a flusher at work? (A live, fresh lock.) */
function lockHeld() { const s = lockState(); return Boolean(s && !s.stale); }

/**
 * Take the lock, or return null. A stale lock is renamed aside first; whoever wins
 * that rename may create the new one. If what we moved aside turns out to be a
 * fresh lock (someone retired the stale one and took it between our look and our
 * rename), it is linked back and we give up.
 */
function acquireLock() {
  const owner = rand(8);
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      fs.mkdirSync(DIR, { recursive: true });
      const fd = fs.openSync(LOCK_FILE, 'wx', 0o600);
      try { fs.writeSync(fd, JSON.stringify({ host: HOST, pid: process.pid, owner, at: Date.now() })); } finally { fs.closeSync(fd); }
      return owner;
    } catch (e) {
      if (e.code !== 'EEXIST') return null;
    }
    const seen = lockState();
    if (seen && !seen.stale) return null;
    if (seen) {
      const aside = `${LOCK_FILE}.${rand(4)}.stale`;
      try { fs.renameSync(LOCK_FILE, aside); } catch { continue; }
      let moved = null;
      try { moved = JSON.parse(fs.readFileSync(aside, 'utf8')); } catch { /* junk */ }
      if ((moved?.owner ?? null) !== (seen.info?.owner ?? null)) {
        try { fs.linkSync(aside, LOCK_FILE); } catch { /* a third taker has it now */ }
        try { fs.unlinkSync(aside); } catch { /* gone */ }
        return null;
      }
      try { fs.unlinkSync(aside); } catch { /* gone */ }
    }
  }
  return null;
}

/** Remove the lock if, and only if, it is still ours. */
function releaseLock(owner) {
  try { if (readLock()?.owner === owner) fs.unlinkSync(LOCK_FILE); } catch { /* gone */ }
}

// --- local log and backoff ----------------------------------------------------

/** Append one line to ~/.agent-office/adapter.log, keeping the newer half when it grows past LOG_MAX_BYTES. */
function adapterLog(message) {
  try {
    fs.mkdirSync(DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, `${nowIso()} ${message}\n`, { mode: 0o600 });
    if (fs.statSync(LOG_FILE).size > LOG_MAX_BYTES) {
      const text = fs.readFileSync(LOG_FILE, 'utf8');
      const tail = text.slice(text.length - Math.floor(LOG_MAX_BYTES / 2));
      fs.writeFileSync(LOG_FILE, tail.slice(tail.indexOf('\n') + 1), { mode: 0o600 });
    }
  } catch { /* a log is never worth an error */ }
}

function backoff(endpoint) {
  const b = readJson(BACKOFF_FILE, null);
  return b && b.url === endpoint?.url ? b : { failures: 0, until: 0 };
}
function inBackoff(endpoint, now = Date.now()) { return backoff(endpoint).until > now; }

function noteFailure(endpoint, status, now = Date.now()) {
  const failures = (backoff(endpoint).failures ?? 0) + 1;
  const base = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** Math.min(failures - 1, 20));
  const until = now + Math.round(base * (0.8 + Math.random() * 0.4));
  try { writeJsonAtomic(BACKOFF_FILE, { url: endpoint.url, failures, until, status: status ?? null }); } catch { /* next try just comes sooner */ }
}

function noteSuccess() {
  try { if (fs.existsSync(BACKOFF_FILE)) fs.unlinkSync(BACKOFF_FILE); } catch { /* harmless */ }
}

/**
 * Send lines, log what the office refused for good, and keep the rest's
 * backoff state. Returns the lines still to be sent.
 */
async function deliverLines(endpoint, lines) {
  const res = await core.deliver(endpoint, lines, {
    connectMs: endpoint.remote ? REMOTE_CONNECT_MS : CONNECT_MS,
    requestMs: endpoint.remote ? REMOTE_REQUEST_MS : REQUEST_MS,
  });
  for (const d of res.dropped) adapterLog(`dropped ${d.lines} event(s): ${d.reason ?? `HTTP ${d.status}`} (not retried)`);
  if (res.done) noteSuccess();
  else noteFailure(endpoint, res.status);
  return res.keep;
}

// --- payload sampling ------------------------------------------------------

/**
 * Record the *shape* of each payload — keys and value types, never string
 * contents — the first SAMPLE_MAX times a new shape appears. Set
 * AGENT_OFFICE_CAPTURE=full to record values too, on your own machine, knowingly;
 * AGENT_OFFICE_CAPTURE=off to record nothing.
 */
function recordShape(harness, event, payload) {
  const mode = process.env.AGENT_OFFICE_CAPTURE ?? process.env.ROVING_OFFICE_CAPTURE;
  if (mode === '0' || mode === 'off') return;
  const file = path.join(DIR, `shapes-${harness}.ndjson`);
  try {
    const existing = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n').filter(Boolean) : [];
    if (existing.length >= SAMPLE_MAX) return;
    const shape = (obj, depth = 0) => {
      if (obj === null || typeof obj !== 'object') return typeof obj;
      if (Array.isArray(obj)) return depth > 2 ? 'array' : [shape(obj[0], depth + 1)];
      const out = {};
      for (const [k, v] of Object.entries(obj)) out[k] = depth > 2 ? typeof v : shape(v, depth + 1);
      return out;
    };
    const line = JSON.stringify({
      at: nowIso(), event,
      shape: shape(payload),
      ...(mode === 'full' ? { payload } : {}),
    });
    if (existing.includes(line)) return;
    fs.mkdirSync(DIR, { recursive: true });
    fs.appendFileSync(file, `${line}\n`, { mode: 0o600 });
  } catch { /* sampling is never worth an error */ }
}

// --- chaining --------------------------------------------------------------

/** Re-run a displaced Rovo hook command with the same stdin, detached and unwatched. */
function chain(command, stdin) {
  if (!command) return;
  try {
    const child = spawn('/bin/sh', ['-c', command], {
      stdio: ['pipe', 'ignore', 'ignore'], detached: true,
    });
    child.on('error', () => {});
    child.stdin.on('error', () => {});
    child.stdin.end(stdin ?? '');
    child.unref();
  } catch { /* nothing we can do, and nothing worth failing for */ }
}

// --- arguments -------------------------------------------------------------

function parseArgs(argv) {
  const out = { harness: null, event: null, chain: null, printProject: false, flush: false };
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--print-project') { out.printProject = true; continue; }
    if (argv[i] === '--flush') { out.flush = true; continue; }
    if (argv[i] === '--chain') { out.chain = argv[++i] ?? null; continue; }
    if (argv[i].startsWith('--chain=')) { out.chain = argv[i].slice(8); continue; }
    rest.push(argv[i]);
  }
  [out.harness, out.event] = rest;
  return out;
}

// --- delivery --------------------------------------------------------------

/** The coalescing window: AGENT_OFFICE_FLUSH_MS, bounded to 0..FLUSH_MS_MAX. */
function flushWindowMs(env = process.env) {
  const raw = env.AGENT_OFFICE_FLUSH_MS;
  if (raw === undefined || raw === '') return FLUSH_MS_DEFAULT;
  const n = Number(raw);
  if (!Number.isFinite(n)) return FLUSH_MS_DEFAULT;
  return Math.max(0, Math.min(FLUSH_MS_MAX, Math.round(n)));
}

function isUrgent(line) {
  try { return URGENT_TYPES.has(JSON.parse(line).type); } catch { return false; }
}

/**
 * Hand the spool to a detached copy of ourselves and forget about it. The hook's
 * job shrinks to an append and a spawn; the network happens in a process nobody
 * is waiting for. `detached` + `unref` + ignored stdio is what makes it survive
 * the harness reaping us.
 */
function spawnFlusher(endpoint) {
  // A flusher holding the lock will see what we just appended: it drains until the
  // spool is empty and looks once more after letting go of the lock.
  if (lockHeld() || inBackoff(endpoint)) return true;
  try {
    const child = spawn(process.execPath, [SEND_BIN, '--flush'], {
      detached: true,
      stdio: 'ignore',
      env: process.env,   // AGENT_OFFICE_URL/TOKEN must survive into the child
    });
    child.unref();
    return true;
  } catch {
    return false;
  }
}

/**
 * Wait up to `windowMs` for more events to pile up in the spool, so one POST
 * carries the burst. Returns at once when nothing is pending, when an urgent
 * event is (or arrives) in the spool, or at the deadline.
 */
async function coalesce(windowMs, deadline) {
  if (!hasPending()) return;
  const end = Math.min(Date.now() + windowMs, deadline);
  let offset = 0;
  for (;;) {
    const text = readText(SPOOL_FILE);
    if (text.length < offset) offset = 0;
    if (splitLines(text.slice(offset)).some(isUrgent)) return;
    offset = text.lastIndexOf('\n') + 1;
    const left = end - Date.now();
    if (left <= 0) return;
    await new Promise((r) => setTimeout(r, Math.min(FLUSH_POLL_MS, left)));
  }
}

/**
 * One send, under the lock. Orphaned sending files are adopted first; then what a
 * previous round could not deliver, or an adopted file, goes first and alone (one
 * file per round, oldest first); otherwise the spool is
 * renamed to our own sending file, atomically, so hooks start a new spool and
 * nothing they write is in two places. A hook whose append opened the old spool
 * just before the rename writes into our file; whatever is there past what we read
 * is kept for the next round once the POST is over, never sent twice and never lost.
 *
 * Ownership is checked before the round and after the POST. A holder that lost the
 * lock (it slept, or another host judged it gone) leaves what is still unsent in its
 * own file for the new holder to adopt, or, if that was adopted already, nothing.
 *
 * @returns {Promise<'empty'|'sent'|'failed'|'lost'>}
 */
async function sendRound(endpoint, owner) {
  if (!heartbeat(owner)) return 'lost';
  adoptOrphans(owner);
  let mine = sendingFile(owner);
  let text = '';
  for (const f of sendingFiles()) {          // ours, oldest first: what failed or was adopted
    if (f.owner !== owner) continue;
    text = readText(f.file);
    if (text) { mine = f.file; break; }
    try { fs.unlinkSync(f.file); } catch { /* gone */ }
  }
  if (!text) {
    try { fs.renameSync(SPOOL_FILE, mine); } catch { return 'empty'; }
    text = readText(mine);
  }
  const read = Buffer.byteLength(text, 'utf8');
  const all = splitLines(text);
  if (all.length > SPOOL_MAX_LINES) adapterLog(`dropped ${all.length - SPOOL_MAX_LINES} oldest event(s): spool over ${SPOOL_MAX_LINES} lines`);
  const lines = all.slice(-SPOOL_MAX_LINES);
  if (!lines.length) { writeLinesAtomic(mine, []); return 'empty'; }
  const keep = await deliverLines(endpoint, lines);
  const owned = heartbeat(owner);
  let late;
  try { late = splitLines(fs.readFileSync(mine).subarray(read).toString('utf8')); } catch {
    return owned ? (keep.length ? 'failed' : 'sent') : 'lost';   // adopted: the adopter has it all
  }
  writeLinesAtomic(mine, [...keep, ...late]);
  if (!owned) return 'lost';
  return keep.length ? 'failed' : 'sent';
}

/**
 * Drain the spool to the office: the detached half of `spawnFlusher`. Only the
 * lock holder sends; anyone else leaves at once, their events in its hands. The
 * holder waits out the coalescing window, sends, and goes round until the spool is
 * empty, a failure worth retrying stops it (those events stay, ahead of newer
 * ones, and the next attempt waits out the backoff), or its time is up — then a
 * successor is started so nothing waits for the next hook. Batches the office
 * refuses for good are dropped and logged, so they cannot wedge the spool.
 */
async function runFlush({ windowMs = flushWindowMs() } = {}) {
  const endpoint = resolveEndpoint();
  if (!endpoint) return;
  const started = Date.now();
  const deadline = started + FLUSH_WATCHDOG_MS - REMOTE_CONNECT_MS - REMOTE_REQUEST_MS - 1000;
  for (;;) {
    const owner = acquireLock();
    if (!owner) return;
    const onExit = () => releaseLock(owner);
    process.once('exit', onExit);
    const beat = setInterval(() => heartbeat(owner), HEARTBEAT_MS);
    beat.unref?.();
    let outcome = 'empty';
    try {
      while (Date.now() < deadline && !inBackoff(endpoint)) {
        await coalesce(windowMs, deadline);
        outcome = await sendRound(endpoint, owner);
        if (outcome !== 'sent') break;
        outcome = 'timeout';
      }
    } finally {
      clearInterval(beat);
      process.removeListener('exit', onExit);
      releaseLock(owner);
    }
    if (outcome === 'lost' || outcome === 'failed' || inBackoff(endpoint) || !hasPending()) return;
    // A hook that appended while we were letting go saw the lock held and spawned
    // nobody: go round again, or hand over if our time is up.
    if (Date.now() >= deadline) { spawnFlusher(endpoint); return; }
  }
}

// --- envelopes --------------------------------------------------------------

/**
 * Map one hook payload to AOP envelopes (spec §3). Pure apart from the state bag,
 * so it is what the tests drive; `main` wraps it in stdin, spool and watchdog.
 *
 * @returns {{lines: string[], state: object}|null} null when there is nothing to say
 */
function envelopes({ harness: harnessName, event: eventArg, payload, state, endpoint, env = process.env }) {
  let mapper;
  try { mapper = require(`../mappers/${harnessName}.cjs`); } catch { return null; }

  const event = eventArg ?? firstString(payload.hook_event_name, payload.event, payload.type) ?? 'unknown';
  const redaction = resolveRedaction({ env });
  const events = mapper.map({
    event,
    payload,
    state,
    redaction,
    endpointId: `${endpoint.pid ?? ''}@${endpoint.url}`,
    helpers: MAPPER_HELPERS,
  }) ?? [];
  if (!events.length) return { lines: [], state, event };

  const version = firstString(payload.cli_version, payload.version);
  let variant;
  try { variant = typeof mapper.variant === 'function' ? mapper.variant({ payload, env }) : null; }
  catch { variant = null; }

  const harness = { name: harnessName };
  if (version) harness.version = version;
  if (variant) harness.variant = variant;

  // Who is at the keyboard, on every session every mapper names (`session.actor`,
  // spec §3.2). Added here rather than in each mapper so no harness can forget it;
  // a mapper that knows better (an identity of its own) sets it and keeps it.
  // `session.label` is the mapper's, untouched.
  // Only a name the person chose (env or `connect --name`). The renderer draws one
  // character per actor per harness, so stamping the OS login on every session
  // would fold two parallel sessions of one person into one character.
  const { actor, source } = resolveActor({ env });
  const chosen = source === 'env' || source === 'file' ? actor : null;
  const withActor = (session) => (chosen && !session.actor ? { ...session, actor: chosen } : session);

  const lines = events.map((ev) => {
    const sess = sessionState(state, ev.session.id);
    sess.lastSeen = Date.now();
    const envelope = {
      aop: AOP_VERSION,
      id: `${ev.session.id}:${++sess.seq}:${crypto.randomBytes(4).toString('hex')}`,
      ts: ev.ts ?? toIso(payload.timestamp),
      seq: sess.seq,
      type: ev.type,
      harness,
      session: wireSession(withActor(ev.session)),
      payload: ev.payload ?? {},
    };
    const project = deriveProject(ev.session.cwd);
    if (project) envelope.project = project;
    if (ev.ext) envelope.ext = ev.ext;
    return JSON.stringify(envelope);
  });
  return { lines, state, event };
}

// --- main ------------------------------------------------------------------

async function main(argv) {
  const args = parseArgs(argv);

  if (args.flush) {
    await runFlush();
    return;
  }

  if (args.printProject) {
    console.log(JSON.stringify({
      project: deriveProject(process.cwd()),
      redaction: resolveRedaction(),
      settingsFile: SETTINGS_FILE,
      aliasFile: ALIAS_FILE,
      aliases: readJson(ALIAS_FILE, null)?.aliases ?? {},
    }, null, 2));
    return;
  }

  if (!args.harness) return;

  const stdin = await readStdin();

  // The displaced hook goes first: it should not pay for our latency.
  chain(args.chain, stdin);

  const endpoint = resolveEndpoint();
  if (!endpoint?.url) return;

  let payload = {};
  try { payload = stdin ? JSON.parse(stdin) : {}; } catch { payload = {}; }
  if (!payload || typeof payload !== 'object') payload = {};

  const event = args.event ?? firstString(payload.hook_event_name, payload.event, payload.type) ?? 'unknown';
  recordShape(args.harness, event, payload);

  const state = loadState(args.harness);
  const mapped = envelopes({ harness: args.harness, event: args.event, payload, state, endpoint });
  if (!mapped) return;
  writeJsonAtomic(stateFile(args.harness), state);
  if (!mapped.lines.length) return;

  // Remote: spool and hand off. Local: send inline, a loopback POST is cheaper than
  // a second Node process.
  if (endpoint.remote) {
    appendSpool(mapped.lines);
    spawnFlusher(endpoint);
  } else if (inBackoff(endpoint)) {
    appendSpool(mapped.lines);
  } else {
    const batch = claimSpool();
    batch.release(await deliverLines(endpoint, [...batch.lines, ...mapped.lines]));
  }
}

/**
 * Run as the hook process: watchdog armed, every exit path is exit 0.
 * Nothing below this line is allowed to take the process down.
 */
function run(argv = process.argv.slice(2)) {
  process.on('uncaughtException', () => process.exit(0));
  process.on('unhandledRejection', () => process.exit(0));
  const watchdog = setTimeout(
    () => process.exit(0),
    argv.includes('--flush') ? FLUSH_WATCHDOG_MS : WATCHDOG_MS,
  );
  watchdog.unref?.();
  main(argv).then(() => process.exit(0), () => process.exit(0));
}

module.exports = {
  run, main, envelopes, parseArgs, sessionState, runFlush,
  acquireLock, releaseLock, lockHeld, flushWindowMs, pendingLines, URGENT_TYPES,
  sendingFile, sendingFiles, adoptOrphans, HOST, HEARTBEAT_MS,
  SPOOL_FILE, LOCK_FILE, LOCK_STALE_MS, LOCK_FOREIGN_STALE_MS, LOCK_ABANDONED_MS, LOG_FILE, BACKOFF_FILE, SEND_BIN,
  WATCHDOG_MS, FLUSH_WATCHDOG_MS,
};
