#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/start_a_server_relay.sh [--foreground] [--status]
      [--force-restart]

Starts the AscendOP GitPartner server relay from an A-side terminal.
By default this script starts a lightweight supervisor in the background.
Use --foreground when you want the service logs to remain visible in the
current terminal.
Use --force-restart to terminate only this checkout's stale launchers,
server processes, and Git helpers before clearing this checkout's locks.

Environment overrides:
  GITPARTNER_ROOT        Repository root to run from.
  GITPARTNER_CONFIG      Config path, default configs/server.json.
  GITPARTNER_ROLE        Role label for lock/pid names, default server.
  GITPARTNER_STRICT_ROOT_FILTER
                         Keep cleanup scoped to this repository root, default 1.
  GITPARTNER_GIT_IDLE_TIMEOUT_SECONDS
                         Wait for same-root Git helpers before lock cleanup,
                         default 30.
EOF
}

MODE="background"
FORCE_RESTART=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --foreground) MODE="foreground" ;;
    --status) MODE="status" ;;
    --force-restart) FORCE_RESTART=1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

resolve_root() {
  if [ -n "${GITPARTNER_ROOT:-}" ]; then
    cd "$GITPARTNER_ROOT"
    return
  fi
  cd /opt/ascendop/git-partner
}

resolve_root
ROOT_REAL="$(pwd -P)"
CONFIG="${GITPARTNER_CONFIG:-configs/server.json}"
ROLE="${GITPARTNER_ROLE:-server}"
STRICT_ROOT_FILTER="${GITPARTNER_STRICT_ROOT_FILTER:-1}"
GIT_IDLE_TIMEOUT_SECONDS="${GITPARTNER_GIT_IDLE_TIMEOUT_SECONDS:-30}"
case "$GIT_IDLE_TIMEOUT_SECONDS" in
  ''|*[!0-9]*) GIT_IDLE_TIMEOUT_SECONDS=30 ;;
esac
START_LOCK_FILE="${GITPARTNER_START_LOCK_FILE:-.partner_state/git-partner-start.lock}"
PID_FILE="work/git-partner-${ROLE}.pid"
SUPERVISOR_PID_FILE="work/git-partner-${ROLE}-supervisor.pid"
LOG_FILE="work/logs/git-partner-${ROLE}.log"
SUPERVISOR_LOG_FILE="work/logs/git-partner-${ROLE}-supervisor.log"
LEGACY_LOCK_FILE="work/git-partner-${ROLE}.lock"
DAEMON_LOCK_FILE=".partner_state/git-partner-${ROLE}.daemon.lock"
DAEMON_LOCK_JSON=".partner_state/git-partner-${ROLE}.daemon.lock.json"
EFFECTIVE_CONFIG=".partner_state/effective-${ROLE}.json"

mkdir -p work/logs .partner_state

if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  . .venv/bin/activate
fi

export PYTHONPATH="${ROOT_REAL}/src:${PYTHONPATH:-}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export GIT_TERMINAL_PROMPT=0
unset GIT_ASKPASS SSH_ASKPASS

if [ "$MODE" != "status" ]; then
  NETWORK_EXPORTS="$(python3 -m limited_remote_partner.core.login_environment shell-exports --config "$CONFIG")"
  if [ -n "$NETWORK_EXPORTS" ]; then
    eval "$NETWORK_EXPORTS"
  fi
  python3 -m limited_remote_partner.core.login_environment report --config "$CONFIG"
fi

if [ "$MODE" != "status" ] && [ ! -s "${ROOT_REAL}/api.txt" ]; then
  echo "GITPARTNER_REPO_TOKEN_MISSING path=${ROOT_REAL}/api.txt" >&2
  echo "place the Gitee token in repository-root api.txt and chmod 600 it" >&2
  exit 8
fi

same_root_pid() {
  local pid="$1"
  [ -n "$pid" ] || return 1
  [ "$pid" != "$$" ] || return 1
  [ "$pid" != "${BASHPID:-$$}" ] || return 1
  if ! kill -0 "$pid" 2>/dev/null; then
    return 1
  fi
  if [ "$STRICT_ROOT_FILTER" != "1" ]; then
    return 0
  fi
  local cwd
  cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"
  [ "$cwd" = "$ROOT_REAL" ]
}

cmdline_of() {
  tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null || true
}

metadata_repo_matches_root() {
  local path="$1"
  [ -f "$path" ] || return 1
  python3 - "$path" "$ROOT_REAL" <<'PY' 2>/dev/null
import json
import os
import sys

path, root = sys.argv[1], os.path.realpath(sys.argv[2])
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
repo = data.get("repo_dir") or data.get("root") or data.get("cwd") or ""
if repo and os.path.realpath(str(repo)) == root:
    raise SystemExit(0)
raise SystemExit(1)
PY
}

pids_from_metadata() {
  local path="$1"
  [ -f "$path" ] || return 0
  metadata_repo_matches_root "$path" || return 0
  python3 - "$path" <<'PY' 2>/dev/null || true
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
for key in ("pid", "process_pid", "server_pid"):
    value = data.get(key)
    if isinstance(value, int):
        print(value)
PY
}

server_pids_in_root() {
  local pid args result=""
  while read -r pid; do
    [ -n "$pid" ] || continue
    same_root_pid "$pid" || continue
    args="$(cmdline_of "$pid")"
    case "$args" in
      *gitpartner-server-supervisor*|*ascendop-gp-server-supervisor*|*start_a_server_relay.sh*)
        continue
        ;;
      *git-partner-server*|*limited_remote_partner.gateway.server*|*limited_remote_partner.server*|*limited_remote_partner.cli.partner*"--role server"*|*limited_remote_partner.partner*"--role server"*|*"git-partner --config"*)
        result="${result}${pid} "
        ;;
    esac
  done <<EOF_PIDS
$(ps -eo pid=)
EOF_PIDS
  for pid in $(pids_from_metadata "$DAEMON_LOCK_JSON"); do
    same_root_pid "$pid" || continue
    result="${result}${pid} "
  done
  printf '%s\n' "$result" | tr ' ' '\n' | awk 'NF && !seen[$1]++'
}

supervisor_pids_in_root() {
  local pid args result=""
  while read -r pid; do
    [ -n "$pid" ] || continue
    same_root_pid "$pid" || continue
    args="$(cmdline_of "$pid")"
    case "$args" in
      *gitpartner-server-supervisor*|*ascendop-gp-server-supervisor*)
        result="${result}${pid} "
        ;;
    esac
  done <<EOF_PIDS
$(ps -eo pid=)
EOF_PIDS
  if [ -f "$SUPERVISOR_PID_FILE" ]; then
    pid="$(cat "$SUPERVISOR_PID_FILE" 2>/dev/null || true)"
    if same_root_pid "$pid"; then
      result="${result}${pid} "
    fi
  fi
  printf '%s\n' "$result" | tr ' ' '\n' | awk 'NF && !seen[$1]++'
}

launcher_pids_in_root() {
  local pid args result=""
  while read -r pid; do
    [ -n "$pid" ] || continue
    same_root_pid "$pid" || continue
    args="$(cmdline_of "$pid")"
    case "$args" in
      *start_a_server_relay.sh*|*start_gitpartner_service.sh*)
        result="${result}${pid} "
        ;;
    esac
  done <<EOF_PIDS
$(ps -eo pid=)
EOF_PIDS
  printf '%s\n' "$result" | tr ' ' '\n' | awk 'NF && !seen[$1]++'
}

git_pids_in_root() {
  local pid args result=""
  while read -r pid; do
    [ -n "$pid" ] || continue
    same_root_pid "$pid" || continue
    args="$(cmdline_of "$pid")"
    case "$args" in
      git\ *|*/git\ *|*git-fetch*|*git-index-pack*|*git-remote-https*)
        result="${result}${pid} "
        ;;
    esac
  done <<EOF_PIDS
$(ps -eo pid=)
EOF_PIDS
  printf '%s\n' "$result" | tr ' ' '\n' | awk 'NF && !seen[$1]++'
}

print_status() {
  echo "GITPARTNER_ROOT=${ROOT_REAL}"
  echo "GITPARTNER_CONFIG=${CONFIG}"
  echo "[server]"
  local servers
  servers="$(server_pids_in_root | tr '\n' ' ')"
  echo "pids: ${servers:-none}"
  if [ -n "$servers" ]; then
    ps -o pid=,ppid=,etime=,args= -p $servers 2>/dev/null || true
  fi
  echo "[supervisor]"
  local supervisors
  supervisors="$(supervisor_pids_in_root | tr '\n' ' ')"
  echo "pids: ${supervisors:-none}"
  if [ -n "$supervisors" ]; then
    ps -o pid=,ppid=,etime=,args= -p $supervisors 2>/dev/null || true
  fi
  echo "[git]"
  local git_processes
  git_processes="$(git_pids_in_root | tr '\n' ' ')"
  echo "pids: ${git_processes:-none}"
  if [ -n "$git_processes" ]; then
    ps -o pid=,ppid=,etime=,args= -p $git_processes 2>/dev/null || true
  fi
  echo "[launchers]"
  local launchers
  launchers="$(launcher_pids_in_root | tr '\n' ' ')"
  echo "pids: ${launchers:-none}"
  if [ -n "$launchers" ]; then
    ps -o pid=,ppid=,etime=,args= -p $launchers 2>/dev/null || true
  fi
}

kill_pids() {
  local label="$1"
  shift || true
  local pids=("$@")
  [ "${#pids[@]}" -gt 0 ] || return 0
  echo "stopping ${label}: ${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 2
  local still=()
  local pid
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      still+=("$pid")
    fi
  done
  if [ "${#still[@]}" -gt 0 ]; then
    echo "force stopping ${label}: ${still[*]}"
    kill -9 "${still[@]}" 2>/dev/null || true
  fi
}

cleanup_git_state() {
  local gpids deadline
  deadline=$((SECONDS + GIT_IDLE_TIMEOUT_SECONDS))
  while :; do
    gpids="$(git_pids_in_root | tr '\n' ' ')"
    [ -n "$gpids" ] || break
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "git processes still active in ${ROOT_REAL}: ${gpids}" >&2
      ps -o pid=,ppid=,etime=,args= -p $gpids 2>/dev/null >&2 || true
      echo "refusing to remove git locks while git is active" >&2
      return 6
    fi
    echo "waiting for same-root git processes: ${gpids}"
    sleep 1
  done
  git rebase --abort >/dev/null 2>&1 || true
  find .git -name '*.lock' -type f -print -delete 2>/dev/null || true
  rm -rf .git/rebase-merge .git/rebase-apply
}

run_service() {
  if command -v git-partner-server >/dev/null 2>&1; then
    exec git-partner-server --config "$CONFIG"
  fi
  exec python3 -m limited_remote_partner.gateway.server --config "$CONFIG"
}

if [ "$MODE" = "status" ]; then
  print_status
  exit 0
fi

exec 9>"$START_LOCK_FILE"
if ! flock -n 9; then
  if [ "$FORCE_RESTART" != "1" ]; then
    echo "start lock is busy: $START_LOCK_FILE" >&2
    echo "retry with --force-restart to replace only this checkout's launcher" >&2
    exit 7
  fi
  mapfile -t old_launchers < <(launcher_pids_in_root)
  kill_pids "same-root launchers holding the start lock" "${old_launchers[@]}"
  if ! flock -w 5 9; then
    echo "force restart could not acquire start lock: $START_LOCK_FILE" >&2
    exit 7
  fi
fi

mapfile -t old_supervisors < <(supervisor_pids_in_root)
kill_pids "old supervisors" "${old_supervisors[@]}"

mapfile -t old_servers < <(server_pids_in_root)
kill_pids "old servers" "${old_servers[@]}"

rm -f "$PID_FILE" "$SUPERVISOR_PID_FILE" "$LEGACY_LOCK_FILE" \
  "$DAEMON_LOCK_FILE" "$DAEMON_LOCK_JSON" "$EFFECTIVE_CONFIG"

if [ "$FORCE_RESTART" = "1" ]; then
  mapfile -t old_git_helpers < <(git_pids_in_root)
  kill_pids "same-root Git helpers" "${old_git_helpers[@]}"
  rm -rf .partner_state/git-operation.lock.d
fi

cleanup_git_state

echo "starting AscendOP GitPartner server from ${ROOT_REAL}"
echo "config: ${CONFIG}"
echo "force_restart: ${FORCE_RESTART}"

if [ "$MODE" = "foreground" ]; then
  echo "mode: foreground"
  run_service
fi

echo "mode: background supervisor"
(
  exec -a ascendop-gp-server-supervisor bash -c '
    set -euo pipefail
    ROOT_REAL="$1"
    CONFIG="$2"
    LOG_FILE="$3"
    PID_FILE="$4"
    cd "$ROOT_REAL"
    if [ -f .venv/bin/activate ]; then
      . .venv/bin/activate
    fi
    export PYTHONPATH="${ROOT_REAL}/src:${PYTHONPATH:-}"
    export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
    export GIT_TERMINAL_PROMPT=0
    unset GIT_ASKPASS SSH_ASKPASS
    while :; do
      echo "[supervisor] start $(date -Is)" >> "$LOG_FILE"
      if command -v git-partner-server >/dev/null 2>&1; then
        git-partner-server --config "$CONFIG" >> "$LOG_FILE" 2>&1 &
      else
        python3 -m limited_remote_partner.gateway.server --config "$CONFIG" >> "$LOG_FILE" 2>&1 &
      fi
      child=$!
      echo "$child" > "$PID_FILE"
      rc=0
      wait "$child" || rc=$?
      echo "[supervisor] exit rc=${rc} $(date -Is)" >> "$LOG_FILE"
      sleep 3
    done
  ' ascendop-gp-server-supervisor "$ROOT_REAL" "$CONFIG" "$LOG_FILE" "$PID_FILE"
) >> "$SUPERVISOR_LOG_FILE" 2>&1 &
echo "$!" > "$SUPERVISOR_PID_FILE"

sleep 5
print_status

servers="$(server_pids_in_root | tr '\n' ' ')"
if [ -z "$servers" ]; then
  echo "server did not stay up; recent logs:" >&2
  tail -80 "$LOG_FILE" "$SUPERVISOR_LOG_FILE" 2>/dev/null || true
  exit 3
fi

echo "AscendOP GitPartner server is running."
