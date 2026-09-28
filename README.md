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
- failure circuit breaking;
- immutable plan/output identity;
- execution status reporting; and
- a hard v0.1 gate rejecting workers with a positive hourly rate.

WLO does not interpret the domain meaning of a workload or its results.

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

A normal rerun or resume never reruns jobs already recorded as `complete` or
`failed`.

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
remote executors. It is a library boundary; paid execution and integration with
the WLO runner remain separate milestones.

```bash
bundle exec rake
script/check
```

See `docs/execution-plan-v0.1.md` for the frozen plan shape and
`docs/architecture.md` for ownership boundaries.
