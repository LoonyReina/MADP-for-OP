#!/usr/bin/env bash
# Public executor boundary for the Scheduler V2 historical reconstruction.
set -uo pipefail

OP="${1:?usage: run_task.sh <op> <task_id>}"
TASK_ID="${2:?need task id}"
TASK_FILE="logs/$OP/task.md"
EXECUTOR="${ASCENDOP_TASK_EXECUTOR:-}"

emit_failure() {
  printf '__SCHEDULER_STATUS=failed\n'
  printf '__SCHEDULER_SUMMARY=%s\n' "$1"
  printf '__SCHEDULER_ARTIFACTS=\n'
  printf '__SCHEDULER_ERRLOG=\n'
}

if [ -z "$EXECUTOR" ]; then
  emit_failure "ASCENDOP_TASK_EXECUTOR is not configured"
  exit 0
fi

if [ ! -x "$EXECUTOR" ]; then
  emit_failure "configured executor is not executable"
  exit 0
fi

rc=0
out=$("$EXECUTOR" "$OP" "$TASK_ID" "$TASK_FILE" 2>&1) || rc=$?
printf '%s\n' "$out"

if ! printf '%s\n' "$out" | grep -q '^__SCHEDULER_STATUS='; then
  emit_failure "executor returned rc=$rc without a scheduler receipt"
fi

exit 0
