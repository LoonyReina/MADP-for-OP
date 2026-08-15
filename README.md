# AscendOP MADP

AscendOP MADP is a protocol-first multi-agent framework for reproducible hardware
operator development across heterogeneous test endpoints.

This first public commit is a **sanitized historical reconstruction** of the
Scheduler V2 source snapshot preserved on 2026-06-02. It is published to make the
project's architectural evolution inspectable; it is not the current production
workflow.

## What Scheduler V2 established

- An append-only task contract shared by producers and a serial scheduler.
- Explicit `pending`, `claimed`, `running`, and terminal task states.
- One hardware lock protecting scarce build and accelerator capacity.
- Structured completion receipts and follow-up notifications.
- A narrow executor boundary that can be implemented by different endpoints.

## Why the architecture evolved

The V2 queue was human-readable and useful for early experiments, but file polling,
manual recovery, and endpoint-specific shell code made durable multi-agent
coordination difficult. Later releases moved state ownership into typed protocol,
daemon, adapter, and engine layers.

## Local demo

The demo uses no accelerator and no remote endpoint:

```bash
chmod +x examples/mock_executor.sh
bash scripts/v2/components/submit_task.sh \
  --op DemoOp --type correctness --version 1 --deploy-verified
ASCENDOP_OPERATORS=DemoOp \
ASCENDOP_TASK_EXECUTOR="$PWD/examples/mock_executor.sh" \
  bash scripts/v2/scheduler/scheduler.sh tick
bash scripts/v2/scheduler/scheduler.sh status
```

See [the task contract](docs/contracts/task_md.md) for the executor receipt format.

## Sanitization boundary

The public history excludes credentials, endpoint identities, private paths,
operator implementations, benchmark artifacts, production queue state, and test
data. Public adapter hooks replace the private prototype's direct remote commands.

## License and trademarks

Licensed under the Apache License 2.0. Ascend and other product names belong to
their respective owners. This independent project is not affiliated with or
endorsed by Huawei or OpenAI.
