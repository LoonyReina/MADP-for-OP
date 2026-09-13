# Participant handoff

This is a collaboration convention, not a new required CLI. Use installed host
capabilities; private commands are not automatically public core features.

1. Agree on one operator workspace and an admitted writer. Transfer ownership
   through the host; don't forge CLIENT availability or edit queue state.
2. Read the brief, phase, case matrix and accepted baseline. Managed BRIEF/CLIENT
   are generated views; notes/scripts belong to the participant's writable area.
3. Choose analysis, source edits or legal inputs. CPU analysis needs no remote
   action and does not prove device correctness.
4. Finish writing before freezing a test request. Keep its identity and recover/
   status/read the same accepted request.
5. Read original results, preserve failures, ACK after consumption, and record
   findings/next steps. Handoff should not require replaying the previous chat.
6. Decide experiment or external submit/hold within user authority. Record
   justified exceptions, without approval for every ordinary experiment.

Suggested **research note**, not a control schema:

```text
stage: correctness | performance-overall | performance-alignment | targeted
candidate: <source revision or frozen candidate reference>
case_matrix: <revision and coverage gaps>
baseline: <externally accepted source; local remeasurement reference>
latest_request: <original ID and result path>
finding: <observation; distinguish hypothesis>
next: <experiment and discriminating outcome>
external_decision: hold | submit; reason and authorization
```

One long-lived solver workspace per operator is normal; a permanent solver/tester
pair is not required. The trusted checker stays outside source/input authoring
authority. An independent host may drive the installed Gateway manually without
native daemon messaging. Managed writers must still respect their actual CLIENT.

For performance, reference [the shared strategy](PERFORMANCE_ITERATION.md), don't
duplicate thresholds in prompts. Report specific missing device capabilities;
continue other useful analysis where possible.
