#!/usr/bin/env bash
# check_writer_idle.sh — main wakeup utility, scan each op's task.md for writer
# idle (no submission, no in-flight task) older than threshold and emit nudge
# records for main to SendMessage.
#
# Output (stdout): one record per writer needing nudge:
#   {"op":"DemoOp","idle_min":17,"last_task":"T20260502-022","last_status":"failed",
#    "last_summary":"build DemoOp 32 failed","reason":"build_fail_no_retry"}
#
# Reasons:
#   no_pending_after_done   — last task done/failed N min ago, no new submission, no chain run
#   build_fail_no_retry     — last build/install/correctness failed, writer didn't retry
#   pmu_data_unread         — last perf-bin-dual done N min ago, no new V_n+1 submitted
#   stuck_running           — task been "running" >SCHEDULER_TASK_TIMEOUT, possibly hung

set -uo pipefail

if [ -n "${ASCENDOP_OPERATORS:-}" ]; then
  read -r -a OPS <<<"$(echo "$ASCENDOP_OPERATORS" | tr ',' ' ')"
else
  OPS=()
  for task_file in logs/*/task.md; do
    [ -f "$task_file" ] || continue
    OPS+=("$(basename "$(dirname "$task_file")")")
  done
fi
NOW=$(date +%s)
IDLE_MIN_THRESHOLD=${WRITER_IDLE_MIN:-10}

for op in "${OPS[@]}"; do
  f="logs/$op/task.md"
  [ -f "$f" ] || continue

  # Parse latest task block
  latest=$(awk '
    /^## task / { tid=""; type=""; status=""; ready_ts=""; completed_ts=""; result=""; summary="" }
    /^task_id:/ { sub(/^task_id:[ \t]*/, ""); tid=$0 }
    /^type:/ { sub(/^type:[ \t]*/, ""); type=$0 }
    /^status:/ { sub(/^status:[ \t]*/, ""); status=$0 }
    /^ready_ts:/ { sub(/^ready_ts:[ \t]*/, ""); ready_ts=$0 }
    /^completed_ts:/ { sub(/^completed_ts:[ \t]*/, ""); completed_ts=$0 }
    /^result:/ { sub(/^result:[ \t]*/, ""); result=$0 }
    /^result_summary:/ { sub(/^result_summary:[ \t]*/, ""); summary=$0 }
    /^---$/ {
      if (tid != "") {
        latest_tid=tid; latest_type=type; latest_status=status;
        latest_ready=ready_ts; latest_completed=completed_ts;
        latest_result=result; latest_summary=summary
      }
    }
    END {
      printf "%s|%s|%s|%s|%s|%s|%s\n",
        latest_tid, latest_type, latest_status,
        latest_ready, latest_completed, latest_result, latest_summary
    }
  ' "$f")

  IFS='|' read -r tid type status ready_ts completed_ts result summary <<<"$latest"

  # Compute idle time from completed_ts (or ready_ts if not completed)
  ts_for_idle="$completed_ts"
  [ -z "$ts_for_idle" ] && ts_for_idle="$ready_ts"
  [ -z "$ts_for_idle" ] && continue

  ts_epoch=$(date -u -d "$ts_for_idle" +%s 2>/dev/null || echo "$NOW")
  idle_sec=$((NOW - ts_epoch))
  idle_min=$((idle_sec / 60))

  [ "$idle_min" -lt "$IDLE_MIN_THRESHOLD" ] && continue

  # Determine reason
  reason=""
  if [ "$status" = "failed" ] && [[ "$summary" == *"build"*"failed"* ]]; then
    reason="build_fail_no_retry"
  elif [ "$status" = "running" ] && [ "$idle_sec" -gt 1800 ]; then
    reason="stuck_running"
  elif [ "$status" = "done" ]; then
    case "$type" in
      perf-bin|perf-bin-dual) reason="pmu_data_unread" ;;
      perf-time) reason="perf_data_unanalyzed" ;;
      correctness) reason="no_chain_to_perf" ;;
      *) reason="no_followup" ;;
    esac
  elif [ "$status" = "pending" ] || [ "$status" = "claimed" ] || [ "$status" = "running" ]; then
    # latest task still in pipeline; writer is correctly idle waiting
    continue
  else
    reason="unknown"
  fi

  # Skip if op has ANY task in pipeline (pending/claimed/running) — writer waits correctly
  any_active=$(grep -cE '^status: (pending|claimed|running)$' "$f" 2>/dev/null | head -1)
  any_active=${any_active:-0}
  [ "$any_active" -gt 0 ] && continue

  printf '{"op":"%s","idle_min":%d,"last_task":"%s","type":"%s","status":"%s","summary":"%s","reason":"%s"}\n' \
    "$op" "$idle_min" "$tid" "$type" "$status" "$summary" "$reason"
done
