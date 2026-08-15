#!/usr/bin/env bash
# scripts/_lib.sh — shared helpers for round_cycle.sh / perf_round.sh / etc.
#
# Source it via:  source "$(dirname "$0")/_lib.sh"
# Then call:
#   log_event_init <script_name>      # once at top of script
#   log_event <phase> <status> [detail]
#
# Writes append-only:
#   stdout:   "[ts=ISO phase=NAME status=START/DONE/FAIL] detail"
#   logs/_events.jsonl:  {ts, script, op, n, phase, status, detail}
#
# Each calling script sets these BEFORE log_event_init:
#   _LE_OP=<op>          # e.g. "DemoOp"
#   _LE_N=<round/perf number>  # numeric

log_event_init() {
  _LE_SCRIPT="$1"
  mkdir -p logs
  # Trap ERR — captures last exit code into _LE_LAST_RC; uses CURRENT_PHASE
  # variable each phase MUST set ("CURRENT_PHASE=build" etc) so failure points
  # to a real phase name not "unknown".
  trap 'log_event "${CURRENT_PHASE:-unknown}" "FAIL" "rc=$?"' ERR
}

log_event() {
  local phase="$1" status="$2" detail="${3:-}"
  local ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "[ts=$ts phase=$phase status=$status] $detail"
  # JSON line; bash printf %q would over-escape, use literal quotes (detail
  # must not contain raw quotes; callers responsible for sanitizing).
  printf '{"ts":"%s","script":"%s","op":"%s","n":%s,"phase":"%s","status":"%s","detail":"%s"}\n' \
    "$ts" "${_LE_SCRIPT:-?}" "${_LE_OP:-?}" "${_LE_N:-0}" "$phase" "$status" "$detail" \
    >> logs/_events.jsonl
}

# ── NPU/install-slot serial lock (Wave 12, 2026-04-30) ───────────────────────
# Convention: perf_round / perf_binary / perf_app / perf_simulator / round_cycle
# acquire this lock at the very top, BEFORE any remote SSH or local install.
#
# Reentrant: parent (e.g. perf_round) holds the lock + sets _NPU_LOCK_HELD env
# → child (e.g. perf_binary spawned inside Phase 4.5) detects env and skips
# its own acquisition. Standalone child invocation re-acquires fresh.
#
# Auto-release: kernel drops fd 9 when bash exits (even on SIGKILL — fd table
# wiped on process death). The trap is best-effort cleanup of the lockfile
# content, not the fd lock itself.
#
# Lock files:
#   /tmp/local_npu.lock   — local (this machine). Prevents concurrent
#                            perf_round / dispatch loops on same machine.
#   /tmp/remote_npu.lock  — remote (set by SSH heredoc inside scripts).
#                            Prevents concurrent setup.py/pip/perf_all/msprof
#                            from different ops or duplicate runs.
#
# Usage:
#   source "$(dirname "$0")/_lib.sh"
#   npu_lock_acquire <script_name>
npu_lock_acquire() {
  local script_name="${1:-unknown}"
  if [ -n "${_NPU_LOCK_HELD:-}" ]; then
    # Parent already holds it (re-entry from spawned child) — skip
    return 0
  fi
  # Portable mkdir-based mutex — works on Linux/macOS/Windows-git-bash (no flock).
  # mkdir is atomic on POSIX + NTFS-via-MSYS, so race-free.
  local lockdir=/tmp/local_npu.lock.d
  local holder_file="$lockdir/holder"
  local waited=0 max_wait=3600 first=1
  while ! mkdir "$lockdir" 2>/dev/null; do
    if [ "$first" = "1" ]; then
      local h
      h=$(cat "$holder_file" 2>/dev/null | head -1)
      # 2026-05-11 fix: detect dead holder (kill -9 didn't trigger trap, lockdir leak)
      local holder_pid=$(echo "$h" | awk '{print $1}')
      if [ -n "$holder_pid" ] && ! kill -0 "$holder_pid" 2>/dev/null; then
        echo "[npu_lock] stale holder PID $holder_pid dead — forcibly releasing lockdir" >&2
        rm -rf "$lockdir" 2>/dev/null
        continue   # retry mkdir
      fi
      echo "[npu_lock] queueing behind: ${h:-(unknown)} — pid=$$ script=$script_name" >&2
      first=0
    fi
    sleep 5
    waited=$((waited + 5))
    if [ "$waited" -ge "$max_wait" ]; then
      echo "ERROR: NPU lock wait timeout 1hr — likely stuck holder $(cat $holder_file 2>/dev/null)" >&2
      echo "  fix: kill holder PID then 'rm -rf $lockdir'" >&2
      return 22
    fi
  done
  # Stamp holder info for diagnostics
  echo "$$ $script_name $(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$holder_file"
  export _NPU_LOCK_HELD=$$
  [ "$first" = "0" ] && echo "[npu_lock] acquired after ${waited}s wait — pid=$$ script=$script_name" >&2
  trap "rm -rf '$lockdir' 2>/dev/null" EXIT INT TERM
  return 0
}
