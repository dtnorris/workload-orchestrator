# workload-orchestrator

`workload-orchestrator` (WLO) is a small Ruby project for deterministic,
resumable execution of declarative command workloads across configured workers.

## Status

WLO v0.1 implements a deliberately small, zero-cost local execution kernel:

- strict JSON execution-plan validation;
- generic pools and opaque command jobs;
- configured workers and required labels;
- optional exact Ollama model/digest readiness checks;
- direct `argv` execution with layered environment overrides;
- per-job stdout/stderr/metadata evidence;
- sticky terminal states and resumable execution;
- cross-process filesystem job claims;
- graceful pause/resume;
- explicit, audited failed-job retry with preserved attempt evidence;
- failure circuit breaking;
- immutable plan/output identity;
- execution status reporting; and
- a hard v0.1 gate rejecting workers with a positive hourly rate.

WLO does not interpret the domain meaning of a workload or its results.

## Provider-neutral workloads

New v0.2 plans declare logical pools and opaque jobs. Select placement separately
with `--execution-profile FILE`; local and fixed remote endpoints use configured
zero-cost workers. RPOF profiles support checks of existing capacity and fail closed on execution
until paid safety, fulfillment, and remote dispatch are implemented. Existing
v0.1 plans and their no-profile resume commands remain supported.

See [the execution-profile contract](docs/execution-profile-v0.1.md) for the
ownership boundary, examples, CLI, and resume rules.

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

## Quick start

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

Inspect status:

```bash
bin/wlo status examples/hello-plan.json \
  --output output/hello-local
```

A normal rerun or resume skips `complete` and `failed` jobs unless a failed
job has been explicitly authorized for retry using `retry-failed`.

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

## Retry failed jobs

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
of `--all`, use one or more `--job JOB_ID` options to retry selected failed jobs.
Selection and a nonblank reason are mandatory. Unknown, duplicate, completed,
running, already-queued, or never-started job selections fail without authorizing
any retry. `--all` selects only currently failed jobs.

The command queues work and leaves the execution paused; it launches no jobs.
Resume with the original plan, workdir, output, and worker configuration.
Resume runs all pending work, including previously unstarted jobs. Completed
jobs and failed jobs not selected for retry remain untouched. Attempt numbers
increase when a new attempt actually starts. Normal resume alone still does
not retry failures.

Before authorizing a retry, WLO copies each selected attempt's `metadata.json`,
`stdout.log`, and `stderr.log` (when present) to
`OUTPUT/attempts/JOB_ID/attempt-N/`. Those snapshots are never overwritten.
`OUTPUT/runs/JOB_ID/` continues to contain the latest attempt. The original
failed metadata remains there until the retry starts; `wlo status` and
`jobs.json` report the authorized job as pending in the meantime.

`execution.json` retains `retry_history`: timestamp, operator-supplied reason,
selected job IDs and prior attempt numbers, archive paths, and the breaker
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

WLO v0.1 rejects any selected worker whose `hourly_rate_usd` is greater than
zero. Paid-resource lifecycle is intentionally outside this first execution
kernel.

## Security boundary

A WLO plan contains commands to execute. Treat execution plans as executable
input and run only plans you trust. WLO invokes `argv` directly and never passes
plan commands through an implicit shell.

## Development

The additive [RPOF client seam](docs/rpof-client-v0.1.md) provides versioned
capability checks and opaque dispatch through the RPOF executable for future
remote executors. [RPOF pool readiness](docs/rpof-readiness.md) connects
`worker-check` to that seam using logical requirements and an existing-fleet target. It is a library boundary; paid execution and integration with
the WLO runner remain separate milestones.

```bash
bundle exec rake
script/check
```

See `docs/execution-plan-v0.1.md` for the frozen plan shape and
`docs/architecture.md` for ownership boundaries.

## Paid-budget foundation

The [paid-budget contract](docs/paid-budget-v0.1.md) provides explicit finite
limits, durable execution binding, and lifecycle coordination with RPOF's
independent guardian. This library foundation does not enable paid execution;
the nonzero-cost-worker and RPOF-profile execution blocks remain in place.
