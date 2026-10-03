# workload-orchestrator

`workload-orchestrator` (WLO) is a small Ruby project for deterministic,
resumable execution of declarative command workloads across configured workers.

## Status

WLO's production runtime executes provider-neutral v0.3 plans against one or
more independently configured dynamic worker registries. WLO owns the frozen
[`dynamic-worker-registry/v0.1`](contracts/dynamic-worker-registry/v0.1/README.md)
public provider API and its canonical fixtures. Generic v0.1 local execution
and v0.2 execution-profile
compatibility remain available during migration. WLO provides:

- strict JSON execution-plan validation;
- generic pools and opaque command jobs;
- configured workers and required labels;
- optional exact Ollama model/digest readiness checks;
- direct `argv` execution with layered environment overrides;
- per-job stdout/stderr/metadata evidence;
- sticky terminal states and resumable execution;
- cross-process filesystem job claims;
- graceful pause/resume;
- scoped foreground cancellation of owned child process trees;
- explicit, audited failed-job retry with preserved attempt evidence;
- failure circuit breaking;
- immutable plan/output identity;
- execution status reporting;
- capability-based dynamic scheduling across heterogeneous workers; and
- Mac-local command execution against the exact selected worker endpoint.

Provider capacity campaigns run independently in RPOF. WLO consumes their
provider-neutral worker registries, along with independently published local
registries, and owns no provider budget, admission, resource creation,
retention or teardown decision.

WLO does not interpret the domain meaning of a workload or its results. AFW
owns AdventureFinder selection, qualification, frozen scoring contracts and
provenance. RPOF owns provider fleets, readiness/bootstrap/tunnels, leases,
cost safeguards, scaling, replacement and teardown.

## Provider-neutral workloads

New v0.3 plans declare logical pools, requirements and opaque jobs. Named
`WorkerSource` entries supply validated registry snapshots; WLO schedules over
their union while preserving each publisher's `registry_id`, revision and
durable checkpoint. WLO never creates a merged or synthetic registry identity.
It selects eligible workers, persists exact attempt bindings and runs each job
command locally with the selected endpoint. Zero compatible workers leaves the
execution alive and polling. Local stdout, stderr, exit status and executor
exceptions determine command results; no RPOF workload request or result
translation participates. The CLI accepts a strict named-source configuration
through `--worker-sources-config`. The single-command
`--worker-source-command` / `--worker-source-arg` form remains a compatibility
path. See [named worker sources](docs/worker-sources.md),
[the operator workflow](docs/operator-workflow.md), and
[the dynamic dispatch boundary](docs/dynamic-dispatch-boundary.md).

Existing v0.1 local plans and v0.2 local/fixed-remote profiles remain supported
for historical compatibility. A v0.3 plan cannot be rebound through an
execution profile; it requires the provider-neutral dynamic worker source.
DW-33 removed WLO-owned RPOF execution. Historical RPOF profiles remain readable
but cannot run or perform provider readiness checks. See
[legacy RPOF compatibility](docs/legacy-rpof-compatibility.md).

See [the legacy execution-profile contract](docs/execution-profile-v0.1.md) for
v0.2 compatibility examples, CLI, and resume rules.

## Operator commands

Use `bin/wlo start` for detached execution and `bin/wlo summary` for a readable
progress/result report. Each launch retains its manager PID, log and exit result.
See [the operator workflow](docs/operator-workflow.md) for pause, detached resume,
retry and JSON compatibility.

## Requirements

- Ruby 4.0.x
- Bundler

## Setup

```bash
bundle install
cp config/workers.example.yml config/workers.yml
```

`config/workers.yml`, `.env`, and `output/` are machine-local and ignored by
Git.

## Generic compatibility example

The bundled `hello-plan.json` demonstrates the retained static v0.1 command
runner. It is not the production AdventureFinder path; new AFW production work
uses an unbound v0.3 plan and a dynamic worker source.

Validate the bundled generic example:

```bash
bin/wlo validate examples/hello-plan.json
```

Inspect the execution plan without running it:

```bash
bin/wlo plan examples/hello-plan.json \
  --workdir "$PWD"
```

Check worker readiness:

```bash
bin/wlo worker-check examples/hello-plan.json
```

Run it:

```bash
bin/wlo run examples/hello-plan.json \
  --workdir "$PWD" \
  --output output/hello-local
```

For foreground `run`/`resume`, Ctrl-C stops new dispatch and cancels every
owned job process group, including descendants, while retaining partial output
and interrupted-attempt evidence. `wlo pause` is different: it stops new
dispatch but lets running jobs finish normally. Neither action changes provider
capacity. See [the operator workflow](docs/operator-workflow.md) for retry and
signal-boundary details.

Inspect status:

```bash
bin/wlo status examples/hello-plan.json \
  --output output/hello-local
```

Watch a live consolidated execution and its last accepted dynamic-worker state:

```bash
bin/wlo watch examples/hello-plan.json \
  --output output/hello-local \
  --interval 1
```

`watch` is read-only. It reads the execution directory, never contacts a
provider, and exits automatically after `completed`, `workload_failed`, or
`interrupted`.
Ctrl-C stops only the watcher.

A normal rerun or resume skips `complete`, `failed` and `interrupted` jobs.
Failed or interrupted work must be explicitly authorized with `retry-failed`;
an execution retaining interrupted attempts fails closed on resume until that
review is recorded.

## Pause and resume

Request a graceful pause:

```bash
bin/wlo pause --output output/hello-local
```

An already-running command is allowed to finish and persist. No new command is
dispatched while the pause sentinel is present.

Resume the same immutable plan:

```bash
bin/wlo resume examples/hello-plan.json \
  --workdir "$PWD" \
  --output output/hello-local
```

If the plan's failure policy trips its circuit breaker, ordinary resume fails
closed. After inspecting the retained evidence, explicitly acknowledge the
breaker to continue pending jobs:

```bash
bin/wlo resume examples/hello-plan.json \
  --workdir "$PWD" \
  --output output/hello-local \
  --acknowledge-circuit-breaker
```

Failed jobs remain terminal; acknowledgement only resets the breaker counters
for still-pending work.

## Retry failed or interrupted jobs

After repairing the cause, use `retry-failed` to queue another attempt. First
pause and wait for the runner to exit with no running jobs. When upgrading an
existing execution to this version, stop the old runner before using retry;
older runners do not participate in the new execution-wide lock.

```bash
bin/wlo retry-failed PLAN.json \
  --workdir /absolute/original/workdir \
  --output /absolute/original/output \
  --all \
  --reason "Repaired the cause after reviewing failed-job logs" \
  --acknowledge-circuit-breaker
```

Supply `--acknowledge-circuit-breaker` only when the breaker is tripped. Instead
of `--all`, use one or more `--job JOB_ID` options to retry selected failed or
interrupted jobs.
Selection and a nonblank reason are mandatory. Unknown, duplicate, completed,
running, conflicting already-queued, or never-started job selections fail without
authorizing any retry. Repeating the exact same queued authorization returns its
existing action without adding history. `--all` selects only currently failed or
interrupted jobs.

Preview exact selection, evidence hashes, archive paths, and breaker state without
mutation by adding `--dry-run --json`. Inspect the durable bounded history with:

```bash
bin/wlo recovery PLAN.json --workdir WORKDIR --output OUTPUT
```

WLO currently has no deterministic retained-metadata repair transformation.
`wlo repair ... --dry-run` reports that boundary without mutation; an actual
repair request fails closed. Plan/workdir identity validation still runs first.

The command queues work and leaves the execution paused; it launches no jobs.
Resume with the original plan, workdir, output, and worker configuration.
Resume runs all pending work, including previously unstarted jobs. Completed
jobs and failed/interrupted jobs not selected for retry remain untouched.
Attempt numbers increase when a new attempt actually starts. Normal resume alone still does
not retry failures or interruptions.

Before authorizing a retry, WLO copies each selected attempt's `metadata.json`,
`stdout.log`, and `stderr.log` (when present) to
`OUTPUT/attempts/JOB_ID/attempt-N/`. Those snapshots are never overwritten.
`OUTPUT/runs/JOB_ID/` continues to contain the latest attempt. The original
failed metadata remains there until the retry starts; `wlo status` and
`jobs.json` report the authorized job as pending in the meantime.

`execution.json` retains revisioned `retry_history` records using
`wlo-recovery-action/v0.1`: deterministic action ID, timestamp,
operator-supplied reason, selected job IDs, prior status/attempt, archive path,
SHA-256 evidence and metadata hashes, interruption uncertainty, and the breaker
state before acknowledgement. Breaker acknowledgement resets counters and
advances its generation while preserving its trip history. Without
acknowledgement, existing counters remain in force.

The execution-wide lock excludes concurrent run/resume/retry operations.
Retry authorization is an atomic state update after all archives succeed.
If copying is interrupted, failures remain terminal; repeating the command
reuses byte-identical snapshots and refuses conflicting snapshots.
The immutable plan and workdir checks remain enforced. Existing v0.1 output
directories are supported without regeneration or metadata deletion.

WLO preserves its own execution evidence only. It cannot make an arbitrary job
idempotent or restore application-owned files outside these logs; inspect any
partial effects before retrying. Continue using this WLO version after queuing
retries because older binaries do not understand retry authorization.

## Worker configuration

Use `--workers-config FILE` or `WLO_WORKERS_CONFIG` to select a machine-local
worker file. Otherwise WLO reads `config/workers.yml`.

Local and fixed-remote worker bindings reject positive declared hourly rates.
Dynamic workers arrive through the provider-neutral registry; WLO does not use
worker configuration as authority to create or retain paid resources.

## Security boundary

A WLO plan contains commands to execute. Treat execution plans as executable
input and run only plans you trust. WLO invokes `argv` directly and never passes
plan commands through an implicit shell.

## Development

The production dynamic path is documented in
[dynamic polling](docs/dynamic-polling.md) and
[dynamic worker loss](docs/dynamic-worker-loss.md). Historical RPOF client and
dispatch documents are retained under
[legacy compatibility](docs/legacy-rpof-compatibility.md).

```bash
bundle exec rake
script/check
```

See `docs/execution-plan-v0.1.md` for the frozen plan shape and
`docs/architecture.md` for ownership boundaries.

## Historical paid-capacity evidence

Historical RPOF profiles, budget declarations, dispatch contracts and persisted
execution state remain inspectable. WLO no longer contains the provider clients,
capacity fulfillment, paid lifecycle or remote runner. The old
`--rpof-executable`, `--paid-budget` and `--authorize-paid-rpof` flags are removed.
Current execution uses the dynamic worker registry; RPOF owns capacity safety.
