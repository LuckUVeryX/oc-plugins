#!/usr/bin/env bash
# Run state for verify-events-mcp: init, use, status, cleanup.
# Every path lives under /tmp/verify-events-mcp/<run-id>. The run owns its hub,
# its home, and its own tmux server on the socket verify-<run-id>. Evidence is
# the only thing cleanup keeps.
set -euo pipefail

SKILL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO=$(cd "$SKILL_DIR/../../.." && pwd)
ROOT=${VERIFY_ROOT:-/tmp/verify-events-mcp}
ENTRY=$REPO/packages/events-mcp/src/main.ts

usage() {
  printf '%s\n' \
    'usage: run.sh init' \
    '       run.sh use <run-dir>' \
    '       run.sh status <run-dir>' \
    '       run.sh cleanup <run-dir>' \
    '' \
    'init prints export lines. Load them with: eval "$(scripts/run.sh init)"'
  exit 2
}

die() {
  printf 'run.sh: %s\n' "$*" >&2
  exit 1
}

quote() {
  printf "'%s'" "${1//\'/\'\\\'\'}"
}

emit() {
  printf 'export %s=%s\n' "$1" "$(quote "$2")"
}

resolve_token() {
  local host=$1 token=${GITLAB_TOKEN:-}
  if [ -z "$token" ] && command -v glab >/dev/null 2>&1; then
    token=$(glab config get token --host "$host" 2>/dev/null || true)
  fi
  printf '%s' "$token"
}

init() {
  umask 077
  [ -f "$ENTRY" ] || die "no events-mcp entry at $ENTRY; run from the oc-plugins checkout"
  local id=${VERIFY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}
  case "$id" in
    *[!A-Za-z0-9._-]*) die "VERIFY_RUN_ID may use letters, digits, dot, underscore, and dash only" ;;
  esac
  local run=$ROOT/$id
  [ -e "$run" ] && die "$run already exists; choose VERIFY_RUN_ID or use a new run"

  local home=$run/home
  local codex_home=$home/.codex
  local work=$run/work
  local hub=$run/hub
  local evidence=$run/evidence
  mkdir -p "$home/.claude" "$codex_home" "$work" "$hub" "$evidence"
  printf 'verify-events-mcp\n' >"$run/.verify-run"
  {
    printf 'VERIFY_GITLAB_HOST=%s\n' "$(quote "${VERIFY_GITLAB_HOST:-gitlab.com}")"
    printf 'VERIFY_GITLAB_PROJECT=%s\n' "$(quote "${VERIFY_GITLAB_PROJECT:-}")"
    printf 'VERIFY_GITLAB_REF=%s\n' "$(quote "${VERIFY_GITLAB_REF:-}")"
    printf 'VERIFY_LARK_APP_ID=%s\n' "$(quote "${VERIFY_LARK_APP_ID:-}")"
    printf 'VERIFY_LARK_CHAT_ID=%s\n' "$(quote "${VERIFY_LARK_CHAT_ID:-}")"
  } >"$run/.target"
  chmod 600 "$run/.target"

  # Harness auth is read from a copy, never from the real home. Only auth.json
  # is copied for Codex; nothing here writes to ~/.codex. Cleanup deletes the
  # copies with the run.
  if [ -f "$HOME/.claude.json" ]; then
    cp "$HOME/.claude.json" "$home/.claude.json"
  else
    printf 'run.sh: no ~/.claude.json; Claude Code will start cold\n' >&2
  fi
  if [ -f "$HOME/.claude/.credentials.json" ]; then
    cp "$HOME/.claude/.credentials.json" "$home/.claude/.credentials.json"
    chmod 600 "$home/.claude/.credentials.json"
  fi
  if [ -f "$HOME/.claude/settings.json" ]; then
    cp "$HOME/.claude/settings.json" "$home/.claude/settings.json"
  fi
  if [ -f "$HOME/.codex/auth.json" ]; then
    cp "$HOME/.codex/auth.json" "$codex_home/auth.json"
    chmod 600 "$codex_home/auth.json"
  else
    printf 'run.sh: no ~/.codex/auth.json; the Codex feature needs one (doctor --codex names the fix)\n' >&2
  fi

  local host=${VERIFY_GITLAB_HOST:-gitlab.com}
  local token
  token=$(resolve_token "$host")

  cat >"$work/.mcp.json" <<JSON
{
  "mcpServers": {
    "events-mcp": {
      "command": "bun",
      "args": ["$ENTRY", "claude"]
    }
  }
}
JSON

  # Codex forwards named variables from its own environment, so the config
  # holds no secret. The launcher below exports the same names.
  cat >"$codex_home/config.toml" <<TOML
[mcp_servers.events]
command = "bun"
args = ["$ENTRY", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 120
env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST", "EVENTS_MCP_HUB_DIR"]
TOML
  chmod 600 "$codex_home/config.toml"

  # bin/env holds the run's credentials. The launchers source it, target.ts
  # reads it when the shell has no token, and cleanup deletes it with bin/. No
  # secret is printed by any command in this file.
  local bin=$run/bin
  mkdir -p "$bin"
  {
    printf '#!/bin/sh\n'
    printf '# Run credentials. Deleted with the run by cleanup.\n'
    printf 'HOME=%s\n' "$(quote "$home")"
    printf 'EVENTS_MCP_HUB_DIR=%s\n' "$(quote "$hub")"
    printf 'GITLAB_HOST=%s\n' "$(quote "$host")"
    printf 'GITLAB_TOKEN=%s\n' "$(quote "$token")"
    printf 'LARK_APP_ID=%s\n' "$(quote "${VERIFY_LARK_APP_ID:-}")"
    printf 'LARK_APP_SECRET=%s\n' "$(quote "${VERIFY_LARK_APP_SECRET:-}")"
    printf 'export HOME EVENTS_MCP_HUB_DIR GITLAB_HOST GITLAB_TOKEN LARK_APP_ID LARK_APP_SECRET\n'
  } >"$bin/env"
  {
    printf '#!/bin/sh\nset -eu\n'
    printf '. %s\n' "$(quote "$bin/env")"
    printf 'MCP_PROTOCOL_NEGOTIATION=legacy\nexport MCP_PROTOCOL_NEGOTIATION\n'
    printf 'exec claude --strict-mcp-config --mcp-config %s --dangerously-load-development-channels server:events-mcp --allowedTools mcp__events-mcp__list_events mcp__events-mcp__watch mcp__events-mcp__watches mcp__events-mcp__gitlab_get mcp__events-mcp__unwatch "$@"\n' "$(quote "$work/.mcp.json")"
  } >"$bin/claude"
  {
    printf '#!/bin/sh\nset -eu\n'
    printf '. %s\n' "$(quote "$bin/env")"
    printf 'exec codex --no-alt-screen -C %s -a never -s danger-full-access "$@"\n' "$(quote "$work")"
  } >"$bin/codex"
  chmod 700 "$bin"
  chmod 600 "$bin/env"
  chmod 700 "$bin/claude" "$bin/codex"

  emit VERIFY_RUN_DIR "$run"
  emit VERIFY_REPO "$REPO"
  emit VERIFY_WORK "$work"
  emit VERIFY_HOME "$home"
  emit VERIFY_CODEX_HOME "$codex_home"
  emit VERIFY_EVIDENCE "$evidence"
  emit EVENTS_MCP_HUB_DIR "$hub"
  emit VERIFY_TMUX_SOCKET "verify-$id"
  emit VERIFY_TMUX_SESSION "verify-$id"
  emit GITLAB_HOST "$host"
  printf 'printf "run %%s\\n" %s >&2\n' "$(quote "$run")"
}

load_run() {
  local run=${1:-}
  [ -n "$run" ] || usage
  [ -f "$run/.verify-run" ] || die "$run is not a verify-events-mcp run"
  case "$(cd "$run" && pwd)" in
    "$ROOT"/*) ;;
    *) die "$run is outside $ROOT" ;;
  esac
  RUN=$(cd "$run" && pwd)
  # The socket and session are derived from the run id, so a run can only ever
  # address its own tmux server and its own session.
  SOCKET="verify-$(basename "$RUN")"
  SESSION=$SOCKET
  HUB=$RUN/hub
  EVIDENCE=$RUN/evidence
}

hub_pid() {
  local log=$HUB/state/hub.log sock=$HUB/run/hub.sock
  [ -f "$log" ] || return 1
  sed -n "s#.*listening at $sock as pid \([0-9][0-9]*\).*#\1#p" "$log" | tail -1
}

owns_pid() {
  local pid=$1 dir=$2
  [ -r "/proc/$pid/environ" ] || return 1
  tr '\0' '\n' <"/proc/$pid/environ" | grep -Fxq "EVENTS_MCP_HUB_DIR=$dir"
}

kill_under() {
  local dir=$1 link pid target found=1 pass=0
  while [ "$found" -eq 1 ] && [ "$pass" -lt 5 ]; do
    pass=$((pass + 1))
    found=0
    for link in /proc/[0-9]*/exe; do
      target=$(readlink "$link" 2>/dev/null || true)
      case "$target" in
        "$dir"/*)
          pid=${link#/proc/}
          pid=${pid%/exe}
          kill "$pid" 2>/dev/null && printf 'stopped %s\n' "$pid"
          found=1
          ;;
      esac
    done
    [ "$found" -eq 1 ] && sleep 1
  done
  return 0
}

stop_codex() {
  local file pid
  for file in "$RUN/home/.codex/app-server-daemon/daemon.pid" \
    "$RUN/home/app-server-daemon/daemon.pid"; do
    [ -f "$file" ] || continue
    pid=$(sed -n 's/.*"pid":\([0-9][0-9]*\).*/\1/p' "$file" | head -1)
    [ -n "$pid" ] || continue
    if readlink "/proc/$pid/exe" 2>/dev/null | grep -Fq "$RUN/home/"; then
      kill "$pid" 2>/dev/null && printf 'stopped Codex daemon %s\n' "$pid"
    fi
  done
  kill_under "$RUN/home"
  return 0
}

stop_hub() {
  local pid
  pid=$(hub_pid) || return 0
  [ -n "$pid" ] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  owns_pid "$pid" "$HUB" || {
    # A reused pid is not a hub. A live hub for another run is left alone and
    # named, never killed.
    if tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | grep -q 'events-mcp'; then
      printf 'left pid %s: it is not this run hub\n' "$pid" >&2
    fi
    return 0
  }
  local i
  for i in 1 2 3 4 5; do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 1
  done
  kill "$pid" 2>/dev/null || true
  for i in 1 2 3 4 5; do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || true
  return 0
}

stop_tmux() {
  # Exact match, so a session whose name starts with this run's is untouched.
  tmux -L "$SOCKET" kill-session -t "=$SESSION" 2>/dev/null || true
  sleep 2
  if tmux -L "$SOCKET" has-session -t "=$SESSION" 2>/dev/null; then
    printf 'left session %s: it did not stop\n' "$SESSION" >&2
    return 0
  fi
  # The socket name is derived from the run id, so this server is this run's
  # server and no other run shares it.
  tmux -L "$SOCKET" kill-server 2>/dev/null || true
  return 0
}

use() {
  load_run "${1:-}"
  # The target is not a credential, so it travels with the run. Credentials
  # stay in bin/env and are never printed: the launchers, target.ts, and
  # mcp-client.mjs read them from there.
  [ -f "$RUN/.target" ] && . "$RUN/.target"
  local host=${VERIFY_GITLAB_HOST:-gitlab.com}
  emit VERIFY_RUN_DIR "$RUN"
  emit VERIFY_REPO "$REPO"
  emit VERIFY_WORK "$RUN/work"
  emit VERIFY_HOME "$RUN/home"
  emit VERIFY_CODEX_HOME "$RUN/home/.codex"
  emit VERIFY_EVIDENCE "$EVIDENCE"
  emit EVENTS_MCP_HUB_DIR "$HUB"
  emit VERIFY_TMUX_SOCKET "$SOCKET"
  emit VERIFY_TMUX_SESSION "$SESSION"
  emit GITLAB_HOST "$host"
  emit VERIFY_GITLAB_HOST "$host"
  emit VERIFY_GITLAB_PROJECT "${VERIFY_GITLAB_PROJECT:-}"
  emit VERIFY_GITLAB_REF "${VERIFY_GITLAB_REF:-}"
  [ -n "${VERIFY_LARK_APP_ID:-}" ] && emit VERIFY_LARK_APP_ID "$VERIFY_LARK_APP_ID"
  [ -n "${VERIFY_LARK_CHAT_ID:-}" ] && emit VERIFY_LARK_CHAT_ID "$VERIFY_LARK_CHAT_ID"
  return 0
}

status() {
  load_run "${1:-}"
  local pid state=none
  pid=$(hub_pid || true)
  if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
    state="alive pid $pid"
  elif [ -n "${pid:-}" ]; then
    state="gone pid $pid"
  fi
  printf 'run      %s\n' "$RUN"
  printf 'hub      %s\n' "$state"
  printf 'socket   %s\n' "$SOCKET"
  printf 'tmux     %s\n' "$(tmux -L "$SOCKET" list-sessions -F '#{session_name}' 2>/dev/null | tr '\n' ' ' || true)"
  printf 'evidence %s\n' "$EVIDENCE"
  find "$EVIDENCE" -maxdepth 1 -type f -printf '         %f\n' 2>/dev/null | sort
}

cleanup() {
  load_run "${1:-}"
  stop_tmux
  stop_codex
  stop_hub
  if [ -f "$HUB/state/hub.log" ]; then
    cp "$HUB/state/hub.log" "$EVIDENCE/hub.log"
  fi
  rm -rf "$RUN/home" "$RUN/work" "$HUB" "$RUN/bin"
  # The run marker and target stay out of the evidence directory, so cleanup
  # removes them and leaves evidence/ alone.
  rm -f "$RUN/.target" "$RUN/.verify-run"
  [ -d "$EVIDENCE" ] || die "cleanup removed the evidence directory"
  printf 'evidence %s\n' "$EVIDENCE"
  find "$EVIDENCE" -maxdepth 1 -type f -printf '  %f\n' | sort
}

case "${1:-}" in
  init) init ;;
  use) use "${2:-}" ;;
  status) status "${2:-}" ;;
  cleanup) cleanup "${2:-}" ;;
  *) usage ;;
esac