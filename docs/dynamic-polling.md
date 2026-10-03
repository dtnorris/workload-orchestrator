# Dynamic polling and idle execution

Dynamic worker discovery and scheduling belong to one WLO execution. Polling
never creates a new execution, job, or attempt. Every named source has an
independent accepted registry checkpoint in the same output directory across
pause and resume.

The production CLI constructs named `CommandWorkerSource` entries from
`--worker-sources-config`. Each entry executes its argv directly once per poll
and passes stdout bytes unchanged to its registry parser. Source-specific
environment and working-directory values apply only to that subprocess. WLO
does not invoke a shell, mutate global `ENV`, or interpret provider/domain
semantics. A nonzero exit or exec failure is a registry-source error; stderr
and exit detail are retained in the fail-closed dispatch-halt message. The
single-command CLI form remains compatible and uses its legacy checkpoint
path.

## Waiting for capacity

The persisted execution status remains `running` while the active runner owns
the execution lock. No new top-level `waiting` state is introduced. The
following combination is the derived waiting-for-capacity condition:

- at least one job is pending;
- `DynamicScheduler` produces no assignment from the accepted READY workers;
  and
- pause, interruption, circuit-breaker, and dispatch-halt controls are absent.

This condition is not terminal and is not a failure. It includes an empty
registry, READY workers that are incompatible with every pending job, and
partial capacity after all currently eligible work has completed or been
claimed.

## Poll and scheduling cycle

`Runner` owns one `WorkerRegistrySet` and one `DynamicScheduler`. The set owns
one poller and checkpoint per named source. Each cycle
performs these actions in order:

1. validate and durably accept each next registry snapshot;
2. reject duplicate publisher `registry_id` values and form a union without
   changing any publisher identity;
3. reconcile each running dynamic attempt against the accepted checkpoint for
   its exact `registry_id`;
4. if dispatch remains allowed, run the DW-11 scheduler against the immutable
   current and READY worker views;
5. durably bind and launch the selected assignments;
6. inspect the authoritative job and execution state; and
7. when work remains, wait for attempt activity or the configured poll interval
   before polling and scheduling again.

A scheduling pass that produces no assignments does not finish the execution
while pending or running jobs remain. A completed attempt wakes the default
wait immediately so newly freed capacity can be reconsidered without waiting
for the full registry cadence. No second polling or scheduling loop is created.

The default registry poll interval is five seconds. The interval and sleeper
remain injectable for deterministic tests. While idle, the default wait checks
control and interruption state at least every 100 milliseconds, preventing a
busy loop while still stopping promptly.

## Local command endpoint binding

Dynamic v0.3 commands run locally through WLO's command executor. Each child
starts a new process group owned exclusively by that attempt; descendants
inherit that group unless the command deliberately detaches them. The child
starts with WLO's ordinary inherited process environment. String values in the
job `env` map set exact values and `null` removes variables. After the selected
worker's running attempt and exact identity are durably recorded, the launcher
sets `AF_OLLAMA_BASE_URL` from that attempt's immutable
`worker_execution_identity.endpoint`. This is the existing AFW/AFSU Ollama
client convention; it overrides the runtime config. The selected endpoint is
applied last and overrides the parent environment or any job value, including
an explicit unset. Other job values and explicit unsets remain intact. The
injected key is included in the attempt's `environment_keys` evidence.

Each subprocess receives its own environment map. WLO never changes global
`ENV`, reselects a worker at launch, or asks RPOF to dispatch the command. A
replacement generation cannot change the original attempt's endpoint; DW-14
still owns in-doubt reconciliation and explicit retry. Static v0.1/v0.2
environment merging remains unchanged.

AFW's generated v0.3 scorer environment enumerates its complete controlled
namespace with strings or explicit unsets, while omitting
`AF_OLLAMA_BASE_URL`. The locally executed adapter therefore inherits WLO's
binding and passes the closed AFW configuration to the scorer. A local registry
endpoint and a dynamically published remote endpoint use the same path and
differ only in this WLO-owned value. Commands/adapters must honor the convention;
WLO cannot enforce routing inside an arbitrary command that deliberately
overrides it after launch.

## Stop and resume rules

The loop continues while pending or running work exists and none of these stop
conditions applies:

- a pause request;
- SIGINT or SIGTERM;
- a tripped circuit breaker;
- any durable dispatch halt, including dynamic worker loss; or
- all jobs becoming terminal.

A pause prevents new assignments, exits polling after already-claimed attempts
drain without signalling them, leaves unclaimed jobs pending, and preserves the
registry checkpoint and attempt evidence. SIGINT/SIGTERM instead wakes this
wait, stops new assignments, and terminates the execution-owned job groups with
bounded TERM-then-KILL handling. Resume uses the existing execution identity
and store, clears the pause through the established resume path, validates the next
snapshot against the durable checkpoint, and restarts the same poll/schedule
cycle. It does not reset attempt history.

Registry contract, freshness, identity, revision, and immutable-revision
violations, duplicate registry namespaces, malformed command output, and
source-command failures remain fatal
and create the existing fail-closed `worker_registry` dispatch halt. They are
never treated as ordinary absence of capacity and never fall back to static
workers or legacy RPOF workload dispatch.

DW-14 in-doubt worker-loss evidence also remains a dispatch halt. Reconciliation
runs before scheduling, and a resulting halt prevents a replacement worker
from receiving the lost attempt. Only an explicit retry can make that job
pending again.
