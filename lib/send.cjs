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

// The flusher's budgets. It is detached, so nobody is waiting on these.
const REMOTE_CONNECT_MS = 2000;
const REMOTE_REQUEST_MS = 4000;
const FLUSH_WATCHDOG_MS = 10000;

const SPOOL_FILE = path.join(DIR, 'spool.ndjson');
const SPOOL_MAX_LINES = 500;

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

function readSpool() {
  try {
    const lines = fs.readFileSync(SPOOL_FILE, 'utf8').split('\n').filter(Boolean);
    return lines.slice(-SPOOL_MAX_LINES);
  } catch { return []; }
}

function appendSpool(lines) {
  try {
    fs.mkdirSync(DIR, { recursive: true });
    fs.appendFileSync(SPOOL_FILE, `${lines.join('\n')}\n`, { mode: 0o600 });
    const kept = readSpool();
    if (kept.length >= SPOOL_MAX_LINES) fs.writeFileSync(SPOOL_FILE, `${kept.join('\n')}\n`, { mode: 0o600 });
  } catch { /* the office simply misses these */ }
}

/**
 * Take exclusive ownership of what is currently spooled, by rename: atomic on
 * POSIX, so exactly one sender wins a given batch and nothing arriving in the
 * meantime is lost.
 */
function claimSpool() {
  const claim = `${SPOOL_FILE}.${process.pid}.claim`;
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

/** Is a sender mid-flight? Judged by a claim file young enough to belong to a living process. */
function flusherIsWorking() {
  try {
    const now = Date.now();
    for (const name of fs.readdirSync(DIR)) {
      if (!name.startsWith(`${path.basename(SPOOL_FILE)}.`) || !name.endsWith('.claim')) continue;
      const { mtimeMs } = fs.statSync(path.join(DIR, name));
      if (now - mtimeMs < FLUSH_WATCHDOG_MS) return true;
    }
  } catch { /* no directory yet: nobody is working */ }
  return false;
}

/**
 * Hand the spool to a detached copy of ourselves and forget about it. The hook's
 * job shrinks to an append and a spawn; the network happens in a process nobody
 * is waiting for. `detached` + `unref` + ignored stdio is what makes it survive
 * the harness reaping us.
 */
function spawnFlusher(endpoint) {
  if (flusherIsWorking() || inBackoff(endpoint)) return true;
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
 * Drain the spool to the office: the detached half of `spawnFlusher`. Drains
 * until empty; stops on the first failure worth retrying (those events stay
 * spooled and the next attempt waits out the backoff). Batches the office
 * refuses for good are dropped and logged, so they cannot wedge the spool.
 */
async function runFlush() {
  const endpoint = resolveEndpoint();
  if (!endpoint) return;
  const deadline = Date.now() + FLUSH_WATCHDOG_MS;
  while (Date.now() < deadline && !inBackoff(endpoint)) {
    const batch = claimSpool();
    if (!batch.lines.length) return;
    const keep = await deliverLines(endpoint, batch.lines);
    batch.release(keep);
    if (keep.length) return;
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
  SPOOL_FILE, LOG_FILE, BACKOFF_FILE, SEND_BIN, WATCHDOG_MS, FLUSH_WATCHDOG_MS,
};
