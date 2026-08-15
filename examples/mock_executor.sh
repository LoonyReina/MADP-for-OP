#!/usr/bin/env bash
# Minimal endpoint adapter used by the public Scheduler V2 demo.
set -euo pipefail

OP="${1:?need operator}"
TASK_ID="${2:?need task id}"
TASK_FILE="${3:?need task file}"

printf 'mock executor accepted %s/%s from %s\n' "$OP" "$TASK_ID" "$TASK_FILE"
printf '__SCHEDULER_STATUS=done\n'
printf '__SCHEDULER_SUMMARY=mock endpoint completed the task\n'
printf '__SCHEDULER_ARTIFACTS=mock://%s/%s\n' "$OP" "$TASK_ID"
printf '__SCHEDULER_ERRLOG=\n'
