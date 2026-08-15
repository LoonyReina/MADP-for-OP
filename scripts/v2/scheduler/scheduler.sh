#!/usr/bin/env bash
# scheduler.sh — NPU serial scheduler v2 (task.md polling)
#
# 轮询 logs/{Op}/task.md，按 priority+ready_ts 拿队头 pending，
# 经 npu_lock_acquire serialize, 调 run_task.sh 执行, 写回结果。
#
# Modes:
#   tick       single tick: pick 1 pending → run → write back. exit 0.
#   daemon     loop tick every 30s until KILL_FILE created.
#   status     show pending/running/done counts per op.
#   list       full task list with status.
#   abort      mark a stuck task aborted (manual recovery).
#
# Long-running pattern: main agent uses ScheduleWakeup → bash scheduler.sh tick.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SET_FIELD="bash $SCRIPT_DIR/set_task_field.sh"
PARSE="bash $SCRIPT_DIR/parse_task.sh"
RUN_TASK="bash $SCRIPT_DIR/run_task.sh"
KILL_FILE="/tmp/scheduler_v2.stop"

discover_operators() {
  if [ -n "${ASCENDOP_OPERATORS:-}" ]; then
    echo "$ASCENDOP_OPERATORS" | tr ',' ' '
    return
  fi

  local task_file
  for task_file in logs/*/task.md; do
    [ -f "$task_file" ] || continue
    basename "$(dirname "$task_file")"
  done
}

read -r -a OPS <<<"$(discover_operators | tr '\n' ' ')"

# ── status / list ─────────────────────────────────────────────────────────────
do_status() {
  echo "=== scheduler v2 status @ $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  for op in "${OPS[@]}"; do
    f="logs/$op/task.md"
    [ -f "$f" ] || { echo "  $op: no task.md"; continue; }
    p=$(grep -c '^status: pending$' "$f" 2>/dev/null | head -1); p=${p:-0}
    c=$(grep -c '^status: claimed$' "$f" 2>/dev/null | head -1); c=${c:-0}
    r=$(grep -c '^status: running$' "$f" 2>/dev/null | head -1); r=${r:-0}
    d=$(grep -c '^status: done$' "$f" 2>/dev/null | head -1); d=${d:-0}
    fl=$(grep -c '^status: failed$' "$f" 2>/dev/null | head -1); fl=${fl:-0}
    a=$(grep -c '^status: aborted$' "$f" 2>/dev/null | head -1); a=${a:-0}
    echo "  $op: pending=$p claimed=$c running=$r done=$d failed=$fl aborted=$a"
  done
}

do_list() {
  for op in "${OPS[@]}"; do
    f="logs/$op/task.md"
    [ -f "$f" ] || continue
    awk '/^## task / { hdr=$0 }
         /^status:/ { print hdr " | " $0 }' "$f" | tail -10
  done
}

# ── tick ──────────────────────────────────────────────────────────────────────
collect_pending() {
  # Echo: "<priority> <ready_ts> <op> <task_id>" lines, sortable
  for op in "${OPS[@]}"; do
    f="logs/$op/task.md"
    [ -f "$f" ] || continue
    awk -v op="$op" '
      /^## task / { tid=""; pri=""; ts=""; dv=""; status="" }
      /^task_id:/ { line=$0; sub(/^task_id:[ \t]*/, "", line); tid=line }
      /^status: pending$/ { status="pending" }
      /^priority:/ { line=$0; sub(/^priority:[ \t]*/, "", line); pri=line }
      /^ready_ts:/ { line=$0; sub(/^ready_ts:[ \t]*/, "", line); ts=line }
      /^deploy_verified:/ { line=$0; sub(/^deploy_verified:[ \t]*/, "", line); dv=line }
      /^---$/ {
        if (status == "pending" && tid != "" && dv == "yes") {
          # priority desc → use 99-pri so sort -n picks higher prio first
          inv = 99 - pri
          printf "%02d %s %s %s\n", inv, ts, op, tid
        }
      }
    ' "$f"
  done
}

claim_one() {
  local op="$1" tid="$2"
  local f="logs/$op/task.md"
  local now; now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  $SET_FIELD "$f" "$tid" status claimed
  $SET_FIELD "$f" "$tid" claimed_ts "$now"
}

start_one() {
  local op="$1" tid="$2"
  local f="logs/$op/task.md"
  local now; now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  $SET_FIELD "$f" "$tid" status running
  $SET_FIELD "$f" "$tid" started_ts "$now"
}

finish_one() {
  local op="$1" tid="$2" status="$3" summary="$4" artifacts="$5" err_log="$6"
  local f="logs/$op/task.md"
  local now; now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  $SET_FIELD "$f" "$tid" completed_ts "$now"
  $SET_FIELD "$f" "$tid" status "$status"
  case "$status" in
    done)         $SET_FIELD "$f" "$tid" result pass;;
    failed)       $SET_FIELD "$f" "$tid" result fail;;
    timeout)      $SET_FIELD "$f" "$tid" result timeout;;
    aborted)      $SET_FIELD "$f" "$tid" result fail;;
  esac
  [ -n "$summary" ] && $SET_FIELD "$f" "$tid" result_summary "$summary"
  [ -n "$artifacts" ] && $SET_FIELD "$f" "$tid" artifacts "$artifacts"
  [ -n "$err_log" ] && $SET_FIELD "$f" "$tid" error_log "$err_log"

  # 写一行进 _history.jsonl 兼容老 dashboard
  printf '{"ts":"%s","type":"task","op":"%s","task_id":"%s","status":"%s","summary":"%s"}\n' \
    "$now" "$op" "$tid" "$status" "$summary" >>logs/_history.jsonl
}

do_tick() {
  pending=$(collect_pending | sort)
  if [ -z "$pending" ]; then
    echo "[$(date -u +%H:%M:%SZ)] tick: no pending"
    return 0
  fi

  # Pick head
  head_line=$(echo "$pending" | head -1)
  read -r prio ts op tid <<<"$head_line"
  echo "[$(date -u +%H:%M:%SZ)] tick: pick $op/$tid (prio=$((99-10#$prio)) ts=$ts)"

  # Claim (即使后面 NPU 锁等不到也保留 claimed 状态，可在长 running 后回 pending)
  claim_one "$op" "$tid"

  # Acquire NPU lock
  source "$SCRIPT_DIR/../../_lib.sh"
  _LE_OP="$op"; _LE_N=0
  unset _NPU_LOCK_HELD  # C4 fix: clear stale state from previous tick
  if ! npu_lock_acquire "scheduler-$op-$tid"; then
    echo "  ⚠ npu_lock_acquire timeout, leave $tid in claimed; will retry next tick"
    return 0
  fi

  # Start
  start_one "$op" "$tid"

  # Run task with hard timeout (C5 fix: prevent SSH hang killing daemon)
  # 20min cap covers worst-case perf-bin 3 case + msopprof; longer = abort
  local out rc
  # SCHEDULER_TASK_TIMEOUT: 默认 1800s (30min)。alive_monitor.sh 实时显示 in-flight task
  # 跑了多久 + 远端 process + npu-smi 状态——所以 timeout 可以放宽。
  out=$(timeout "${SCHEDULER_TASK_TIMEOUT:-1800}" bash "$SCRIPT_DIR/run_task.sh" "$op" "$tid" 2>&1) || rc=$?
  rc=${rc:-0}
  if [ "$rc" = "124" ]; then
    echo "  ⚠ run_task timed out (>${SCHEDULER_TASK_TIMEOUT:-1800}s), aborting"
    out="${out}
__SCHEDULER_STATUS=timeout
__SCHEDULER_SUMMARY=run_task hit ${SCHEDULER_TASK_TIMEOUT:-1800}s timeout
__SCHEDULER_ARTIFACTS=
__SCHEDULER_ERRLOG="
  fi

  # Parse run_task output (last 4 lines: STATUS / SUMMARY / ARTIFACTS / ERR_LOG)
  status=$(echo "$out" | grep -oE '^__SCHEDULER_STATUS=.*' | sed 's/^__SCHEDULER_STATUS=//' | tail -1)
  summary=$(echo "$out" | grep -oE '^__SCHEDULER_SUMMARY=.*' | sed 's/^__SCHEDULER_SUMMARY=//' | tail -1)
  artifacts=$(echo "$out" | grep -oE '^__SCHEDULER_ARTIFACTS=.*' | sed 's/^__SCHEDULER_ARTIFACTS=//' | tail -1)
  err_log=$(echo "$out" | grep -oE '^__SCHEDULER_ERRLOG=.*' | sed 's/^__SCHEDULER_ERRLOG=//' | tail -1)
  [ -z "$status" ] && { status="failed"; summary="run_task no STATUS line, rc=$rc"; }

  finish_one "$op" "$tid" "$status" "$summary" "$artifacts" "$err_log"

  # Lock auto-released by trap on script exit; manual release here if loop continues
  rm -rf /tmp/local_npu.lock.d 2>/dev/null
  unset _NPU_LOCK_HELD
  echo "  ✓ $op/$tid → $status"
}

do_daemon() {
  # PID guard: refuse to start if another daemon is alive (2026-05-11 多 daemon race fix)
  local PID_FILE="/tmp/scheduler_v2.pid"
  if [ -f "$PID_FILE" ]; then
    local old_pid=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
      echo "ERROR: another daemon alive at PID $old_pid (PID_FILE=$PID_FILE). Refuse to start."
      echo "  → kill it first: kill -9 $old_pid && rm $PID_FILE"
      exit 5
    fi
  fi
  echo $$ > "$PID_FILE"
  trap 'rm -f "$PID_FILE"' EXIT
  # SCHEDULER_POLL_INTERVAL: 默认 75s (60-90s 区间中心)。queue 非空时只 sleep 5s，
  # 让背靠背 task 不被 poll 间隔卡。queue 空时才用 60-90s 节流。
  local IDLE_INT="${SCHEDULER_POLL_INTERVAL:-75}"
  local BUSY_INT=5
  echo "scheduler v2 daemon start. PID=$$ idle_poll=${IDLE_INT}s busy_poll=${BUSY_INT}s KILL_FILE=$KILL_FILE"
  rm -f "$KILL_FILE"
  local WRITER_CHECK_INT="${WRITER_CHECK_INT:-300}"  # 5 min
  local last_writer_check=0
  while :; do
    [ -f "$KILL_FILE" ] && { echo "scheduler kill-switch detected, exit"; rm -f "$KILL_FILE"; break; }
    pre=$(collect_pending | wc -l)
    do_tick
    # 2026-05-11 用户指令: daemon 顺带跑 writer idle check (5 min 间隔)
    now=$(date +%s)
    if [ $((now - last_writer_check)) -ge "$WRITER_CHECK_INT" ]; then
      idle=$(WRITER_IDLE_MIN="${WRITER_IDLE_MIN:-15}" bash "$SCRIPT_DIR/../main/check_writer_idle.sh" 2>/dev/null)
      if [ -n "$idle" ]; then
        echo "[$(date -u +%H:%M:%SZ)] writer-idle:"
        echo "$idle" | sed 's/^/  /'
      fi
      last_writer_check=$now
    fi
    if [ "$pre" -gt 0 ]; then
      sleep "$BUSY_INT"
    else
      sleep "$IDLE_INT"
    fi
  done
}

# ── main ──────────────────────────────────────────────────────────────────────
MODE="${1:-tick}"
case "$MODE" in
  tick)    do_tick ;;
  daemon)  do_daemon ;;
  status)  do_status ;;
  list)    do_list ;;
  *)       echo "usage: scheduler.sh tick|daemon|status|list"; exit 2;;
esac
