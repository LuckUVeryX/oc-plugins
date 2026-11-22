#!/usr/bin/env bash
# tmux driver for harness TUIs. Every session lives on the run's own socket,
# verify-<run-id>, and is addressed by exact name.
set -euo pipefail

SOCKET=${VERIFY_TMUX_SOCKET:-}
SESSION=${VERIFY_TMUX_SESSION:-}
TIMEOUT=${VERIFY_TUI_TIMEOUT:-90}
# Session commands take =name; pane commands need a window spec, so =name:.
SESSION_TARGET="=$SESSION"
PANE_TARGET="=$SESSION:"

usage() {
  printf '%s\n' \
    'usage: tui.sh claude' \
    '       tui.sh codex' \
    '       tui.sh prompt [file]   (reads stdin without a file)' \
    '       tui.sh expect <regex> [seconds]' \
    '       tui.sh key <tmux-key>...' \
    '       tui.sh capture <file>' \
    '       tui.sh stop' \
    '' \
    'Load a run first: eval "$(scripts/run.sh init)"'
  exit 2
}

die() {
  printf 'tui.sh: %s\n' "$*" >&2
  exit 1
}

need_run() {
  [ -n "$SESSION" ] && [ -n "$SOCKET" ] ||
    die 'load a run first: eval "$(scripts/run.sh use <run-dir>)"'
  case "$SOCKET" in
    verify-*) ;;
    *) die "socket $SOCKET is not a run socket" ;;
  esac
  for name in VERIFY_WORK VERIFY_HOME EVENTS_MCP_HUB_DIR; do
    [ -n "${!name:-}" ] || die "$name is empty; load a run first"
  done
}

pane() {
  tmux -L "$SOCKET" capture-pane -p -t "$PANE_TARGET" -S -400 2>/dev/null || true
}

wait_for() {
  local pattern=$1 seconds=${2:-$TIMEOUT} deadline=$((SECONDS + ${2:-$TIMEOUT}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if pane | grep -Eq -- "$pattern"; then
      return 0
    fi
    sleep 0.5
  done
  printf 'tui.sh: no match for /%s/ in %s after %ss\n' "$pattern" "$SESSION" "$seconds" >&2
  pane | tail -40 >&2
  return 1
}

maybe_for() {
  local pattern=$1 seconds=${2:-15} deadline=$((SECONDS + ${2:-15}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if pane | grep -Eq -- "$pattern"; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

key() {
  tmux -L "$SOCKET" send-keys -t "$PANE_TARGET" "$@"
}

start() {
  local dir=$1 command=$2
  tmux -L "$SOCKET" has-session -t "$SESSION_TARGET" 2>/dev/null && die "session $SESSION already exists"
  # Run bash, not the operator's default shell: zsh's first-run wizard in a
  # fresh HOME would eat the typed launcher path.
  tmux -L "$SOCKET" new-session -d -x 200 -y 50 -s "$SESSION" -c "$dir" -- \
    env SHELL=/bin/bash /bin/bash --noprofile --norc -i
  key "$command" Enter
}

claude() {
  need_run
  start "$VERIFY_WORK" "$VERIFY_RUN_DIR/bin/claude"

  # First launch in a fresh home. The default selection is "No, exit".
  if maybe_for 'Quick safety check' 30; then
    key Down
    sleep 0.5
    key Enter
  fi
  # Development channels are confirmed on every launch. Option 1 is selected.
  wait_for 'Loading development channels' 60
  key Enter
  wait_for 'Channels \(experimental\) messages from server' 60
  printf 'ready: %s\n' "$SESSION"
}

codex() {
  need_run
  # The launcher exports the names Codex forwards and leaves CODEX_HOME unset,
  # so Codex uses the default ~/.codex under the isolated HOME.
  start "$VERIFY_WORK" "$VERIFY_RUN_DIR/bin/codex"
  wait_for 'Ask Codex to do anything' 90
  printf 'ready: %s\n' "$SESSION"
}

prompt() {
  need_run
  local text
  if [ "$#" -gt 0 ]; then
    text=$(cat "$1")
  else
    text=$(cat)
  fi
  [ -n "$text" ] || die 'empty prompt'
  tmux -L "$SOCKET" send-keys -l -t "$PANE_TARGET" "$text"
  # The first Enter lands while the composer is still settling on some builds.
  # The second is harmless on an empty composer.
  sleep 1
  key Enter
  sleep 1
  key Enter
}

capture() {
  need_run
  local file=${1:?capture needs a file}
  mkdir -p "$(dirname "$file")"
  pane >"$file"
  printf 'captured %s\n' "$file"
}

case "${1:-}" in
  claude) claude ;;
  codex) codex ;;
  prompt) shift; prompt "$@" ;;
  expect) wait_for "${2:?expect needs a pattern}" "${3:-}" ;;
  key) shift; key "$@" ;;
  capture) capture "${2:-}" ;;
  stop) tmux -L "$SOCKET" kill-session -t "$SESSION_TARGET" 2>/dev/null || true ;;
  *) usage ;;
esac
