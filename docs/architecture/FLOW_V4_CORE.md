# Flow V4 core milestone

Flow V4 closes the Agent-side control loop around durable actions.

## Action lifecycle

```text
queued -> claimed -> running -> completed
                    |    |         |
                    |    |         +-> typed receipt and promoted outputs
                    |    +-> failed / cancelled
                    +-> uncertain -> adopted or retry-pending
```

The exact stored lifecycle also records attempts, lease identity, heartbeats,
session identity, output contracts, and completion evidence. Retry decisions are
central policy; an Agent or carrier does not invent a retry by sending another
turn.

## Authority split

- Agent: reason, edit declared workspace, produce contracted output.
- Agent runner: claim, isolate, execute, heartbeat, collect, and report.
- Control store: serialize ownership and preserve durable state.
- Daemon control plane: choose the current gate and materialize allowed actions.
- Executor adapter: perform approved external work and return evidence.
- Steward: handle a typed capability gap only after normal registered operations
  cannot resolve it.

The public milestone implements the first four responsibilities and the
executor-facing ports. AscendOP owns the concrete GP/Engine adapters, endpoint
pipelines, and steward deployment.
