# ascendop-daemon

This package publishes the modern, reusable control-plane slice of the MADP
daemon: Agent action coordination, scheduling, retry arbitration, durable
storage integration, executor transport contracts, observability, registries,
and workflow gate composition.

It is intentionally not a standalone AscendOP deployment. Service launch
entrypoints, machine configuration, the historical compatibility bridge, live
endpoint bindings, credentials, and runtime state remain private. Four modern
modules that still depend on the compatibility bridge are excluded by the
executable publication manifest.

The public package keeps the `ascendop_daemon` namespace so the extracted code
can be compared with and consumed by the reference implementation.
