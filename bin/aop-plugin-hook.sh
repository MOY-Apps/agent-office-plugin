#!/bin/sh
# The hook command a Claude Code or Codex plugin runs. One manifest serves both
# hosts: Codex exports PLUGIN_ROOT (and a CLAUDE_PLUGIN_ROOT alias); Claude Code
# exports only the latter.
#
# Forked from The Roving Office's bin/aop-plugin-hook.sh (Apache-2.0; see NOTICE).
#
# Nothing in this chain relies on the executable bit: hooks.json says `sh "..."`
# and so does the exec below, because a published plugin is unpacked by the host
# and whether unix modes survive is the extractor's business. A hook that cannot
# be executed fails silently, and the only symptom is an empty office.
#
# Two layouts are served. A packed plugin (what `agent-office install claude`
# stages, and what the marketplace repo publishes) carries bin/, lib/ and
# mappers/ beside this file. A checkout of the source package keeps this file
# under plugin/bin/ with the real package one level up.

set -eu

plugin_root=${PLUGIN_ROOT:-${CLAUDE_PLUGIN_ROOT:-}}
[ -n "$plugin_root" ] || exit 0

if [ -n "${PLUGIN_ROOT:-}" ]; then
  harness=codex-cli
else
  harness=claude-code
fi

if [ -f "$plugin_root/bin/aop-send.cjs" ]; then
  pkg="$plugin_root"
elif [ -f "$plugin_root/../bin/aop-send.cjs" ]; then
  pkg="$plugin_root/.."
else
  exit 0
fi

exec sh "$pkg/bin/aop-node.sh" "$pkg/bin/aop-send.cjs" "$harness"
