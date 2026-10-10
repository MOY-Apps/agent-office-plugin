#!/bin/sh
# aop-node — find a Node, then run an adapter script with it.
#
# Forked from The Roving Office's bin/aop-node.sh (Apache-2.0; see NOTICE).
#
# Every hook command goes through here instead of relying on `#!/usr/bin/env node`,
# because a hook does not get the PATH you think it does: a GUI-launched agent
# (Claude Desktop, Cowork) inherits launchd's PATH, which has no version-managed
# Node on it. So `env node` exits 127 and the office stays empty for exactly those
# sessions - silently, because a non-blocking hook's failure is not the agent's
# problem. The fix is to look where version managers actually put Node, and to
# cache the answer in ~/.agent-office/node-path.
#
#   aop-node.sh <script> [args...]
#
# Exits 0 whatever happens, including when no Node can be found. A failure leaves
# one line in ~/.agent-office/node-missing.log.

set -u

OFFICE_DIR="${AGENT_OFFICE_HOME:-${HOME}/.agent-office}"
CACHE="${OFFICE_DIR}/node-path"

newest_under() {
  root="$1"
  suffix="$2"
  [ -d "$root" ] || return 0
  if printf 'v1\nv2\n' | sort -V >/dev/null 2>&1; then
    order="sort -Vr"
  else
    order="sort -r"
  fi
  ls "$root" 2>/dev/null | $order | while IFS= read -r version
  do
    [ -n "$version" ] || continue
    printf '%s/%s/%s\n' "$root" "$version" "$suffix"
  done
}

candidates() {
  # An explicit override wins (AGENT_OFFICE_NODE; AOP_NODE is the upstream name).
  [ -n "${AGENT_OFFICE_NODE:-}" ] && printf '%s\n' "$AGENT_OFFICE_NODE"
  [ -n "${AOP_NODE:-}" ] && printf '%s\n' "$AOP_NODE"

  command -v node 2>/dev/null

  [ -r "$CACHE" ] && cat "$CACHE" 2>/dev/null

  printf '%s\n' \
    /opt/homebrew/bin/node \
    /usr/local/bin/node \
    /usr/bin/node \
    "${HOME}/.volta/bin/node" \
    "${HOME}/.local/share/fnm/aliases/default/bin/node" \
    "${HOME}/Library/Application Support/fnm/aliases/default/bin/node" \
    "${HOME}/n/bin/node"

  newest_under "${HOME}/.nvm/versions/node" bin/node
  newest_under "${HOME}/.local/share/fnm/node-versions" installation/bin/node
  newest_under "${HOME}/Library/Application Support/fnm/node-versions" installation/bin/node
  newest_under "${HOME}/.asdf/installs/nodejs" bin/node
  newest_under "${HOME}/.local/share/mise/installs/node" bin/node
}

NODE=''
while IFS= read -r candidate
do
  [ -n "$candidate" ] || continue
  if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
    NODE="$candidate"
    break
  fi
done <<CANDIDATES
$(candidates)
CANDIDATES

if [ -z "$NODE" ]; then
  {
    mkdir -p "$OFFICE_DIR" &&
    printf '%s no node found for the office adapter; set AGENT_OFFICE_NODE to one. PATH=%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo '(no date)')" "${PATH:-}" \
      > "${OFFICE_DIR}/node-missing.log"
  } 2>/dev/null || true
  exit 0
fi

OVERRIDE="${AGENT_OFFICE_NODE:-${AOP_NODE:-}}"
if [ "$NODE" != "$OVERRIDE" ] &&
   { [ ! -r "$CACHE" ] || [ "$(cat "$CACHE" 2>/dev/null)" != "$NODE" ]; }
then
  { mkdir -p "$OFFICE_DIR" && printf '%s\n' "$NODE" > "$CACHE"; } 2>/dev/null || true
fi

exec "$NODE" "$@"
