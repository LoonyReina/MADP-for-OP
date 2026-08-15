# Scheduler V2 task contract

Scheduler V2 used an append-only `logs/<operator>/task.md` file as its queue and
receipt ledger. A producer appends a task block, while the scheduler may update
only the fields listed below.

## Producer-owned fields

- `task_id`
- `type`
- `priority`
- `op`
- `version`
- `vendor`
- `cases`
- `msprof_flags`
- `deploy_verified`
- `ready_ts`
- `notes`

## Scheduler-owned fields

- `status`
- `claimed_ts`
- `started_ts`
- `completed_ts`
- `result`
- `result_summary`
- `artifacts`
- `error_log`

The lifecycle is `pending -> claimed -> running -> done|failed|timeout|aborted`.
Only tasks with `deploy_verified: yes` are eligible for execution.

The public reconstruction delegates endpoint work to the executable referenced by
`ASCENDOP_TASK_EXECUTOR`. It receives `<operator> <task-id> <task-file>` and returns
four line-oriented receipt fields:

```text
__SCHEDULER_STATUS=done|failed|timeout|aborted
__SCHEDULER_SUMMARY=<single-line summary>
__SCHEDULER_ARTIFACTS=<artifact reference>
__SCHEDULER_ERRLOG=<error-log reference>
```

An optional `ASCENDOP_ABORT_HOOK` receives `<task-file> <task-id>` when a task is
marked aborted. Endpoint credentials and process-selection policy are deliberately
outside this repository.
