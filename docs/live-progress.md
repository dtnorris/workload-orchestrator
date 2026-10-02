# Unified dynamic execution progress

Dynamic runs and resumes print one plain scrollback stream for the existing
execution. Static run output, JSON `status`, and human `summary` remain unchanged.
There is no full-screen mode, ANSI redraw, extra terminal dependency, provider
query, or separate watch command.

Each invocation begins with `EXECUTION <plan_id>` or `RESUME <plan_id>`. Progress
counts and terminal percentage come from the same persisted `ExecutionReport`
used by status/summary. Historical terminal jobs are counted, not replayed as
new completion events. Existing running attempts are annotated on startup.

Lifecycle lines use opaque job, group, pool, worker, generation, and attempt
identifiers already available to WLO:

- `START`: a running attempt has been persisted before command launch.
- `DONE`: the durable attempt completed successfully.
- `FAIL`: the durable attempt failed.
- `IN_DOUBT`: DW-14 recorded worker-loss evidence; late command completion does
  not turn this into a successful result.

Worker availability comes only from the poller's accepted immutable workers.
READY totals are split into busy and idle by comparing full execution identities
with persisted running attempts. Worker rows show advertised model names and
compatible logical pools. A reused endpoint or worker ID does not make a new
generation busy on behalf of its predecessor.

Aggregate and worker updates print only when their displayed values change.
Worker detail updates print changed rows, bounded to ten; initial running
annotations are also bounded to ten. An unchanged poll, including a newer
revision with the same availability, adds no output.

`status`, `summary`, and `watch` expose additive per-pool status derived from the
last accepted registry checkpoint and WLO's execution evidence. `compatible`
means an exact pool capability match; `busy` means that exact worker generation
is bound to a running attempt; `idle` means a compatible READY generation has no
running binding. WLO never infers provider-resource state.

Pool reasons use deterministic precedence: terminal pool state; pause; circuit
breaker; dispatch/registry halt; running work; runnable idle capacity; busy
compatible capacity; then absent, NOT_READY, or incompatible capacity. Pending
jobs blocked only by dependencies use `NO_RUNNABLE_WORK`, not a capacity reason.
The complete reason set is `RUNNING`, `READY_TO_DISPATCH`,
`NO_COMPATIBLE_READY_WORKERS`, `ALL_COMPATIBLE_WORKERS_BUSY`,
`WORKERS_NOT_READY`, `READY_WORKERS_INCOMPATIBLE`, `PAUSED`,
`CIRCUIT_BREAKER`, `DISPATCH_HALTED`, `NO_ACCEPTED_REGISTRY_SNAPSHOT`,
`REGISTRY_INVALID_OR_STALE`, `COMPLETE`, `FAILED`, `INTERRUPTED`, `NO_PENDING_WORK`, and
`NO_RUNNABLE_WORK`.

Pause/drain, interruption, breaker, and dispatch-halt messages take precedence
over capacity waiting. A worker-loss halt includes its durable reason and the
original attempt's identity in the `IN_DOUBT` line. Explicit retry remains
required; a replacement is not presented as continuation of the lost attempt.

`LiveExecutionReport` supplies a read-only presentation document.
`LiveExecutionDisplay` retains only the previous document for output suppression
and consumes accepted workers supplied by the runner. These primitives are
available for a future DW-21 attachment command; they do not poll, schedule,
retry, or mutate execution state.
