# Local operator workflow

WLO owns starting, observing, pausing, resuming and retrying generic workloads.
These commands use the same plan, execution lock, job claims and breaker as
foreground execution. Production v0.3 runs use a provider-neutral dynamic
worker registry; execution profiles and their worker checks or paid-budget
options apply only to v0.1/v0.2 compatibility paths.

## Command ownership and human control

The operator-facing classifications below describe process ownership, not
domain ownership:

- **FOREGROUND WORK OWNER**: the invoking CLI is the active runner;
- **DETACHED WORK LAUNCHER**: the CLI starts a separately owned process and
  returns;
- **READ-ONLY OBSERVER** / **ONE-SHOT INSPECTION**: the command does not own or
  cancel execution; and
- **CONTROL REQUEST**: the command changes retained WLO control/evidence state
  but does not own provider resources.

| Command | Classification | What remains after return | Ctrl-C / terminal loss | Intentional different action |
| --- | --- | --- | --- | --- |
| `run`, `resume` | FOREGROUND WORK OWNER | No WLO manager is detached. | Ctrl-C stops new dispatch, wakes polling, sends TERM to each execution-owned job process group, escalates surviving groups to KILL after one second, reaps direct children, and retains partial output plus `interrupted` attempt evidence. A second Ctrl-C skips the remaining grace interval. | Use `pause --output OUTPUT` for a graceful drain that never signals running jobs. Use provider tooling separately for capacity teardown. |
| `start` | DETACHED WORK LAUNCHER | The manager owns execution and continues in its own Unix session after the initiating CLI or terminal exits. | Ctrl-C after startup acknowledgement affects only the shell, not the manager. Abrupt manager death has no crash recovery or child-cancellation guarantee. | Use `pause` for WLO work and the applicable provider command for capacity. |
| `watch` | READ-ONLY OBSERVER | Any foreground or detached WLO runner, jobs, publisher processes, and provider resources continue unchanged. | Ctrl-C closes only the view. It does not pause/cancel work and does not tear down capacity. | Use `pause` or the applicable provider teardown command. |
| `status`, `summary` | ONE-SHOT INSPECTION | All existing execution and provider processes/resources continue. | Interrupting the request has no lifecycle meaning. | Use `pause` or provider teardown explicitly. |
| `pause` | CONTROL REQUEST | Running jobs and their runner remain until the existing pause contract drains them; pending jobs stay retained. | Ctrl-C after the request is durable does not strengthen it. The command never signals provider resources. | Wait for `running=0` and `executor inactive`; use provider tooling separately for capacity teardown. |
| `retry-failed` | CONTROL REQUEST | Execution remains paused with selected failures queued. No runner is launched. | Interrupting the CLI is not execution cancellation or provider teardown. | Use `start --resume` or `resume` to run queued work. |
| `import-terminal` | CONTROL REQUEST | Imported terminal evidence remains; no command or runner is launched. | Interrupting the request does not stop other work or resources. | Use normal WLO run/resume and provider lifecycle commands separately. |
| `validate`, `plan`, `worker-check` | ONE-SHOT INSPECTION | No WLO execution owner is created. | Ctrl-C only interrupts the inspection/check. | Use `run`/`start` to execute; use publisher/provider tooling to change capacity. |

The component boundary is absolute: a WLO pause, foreground cancellation,
runner exit, shell exit, or terminal loss is not a provider teardown request.
WLO never proves provider absence. Use the applicable provider's own lifecycle
and verification tooling.

## Production v0.3 dynamic execution

Establish any needed capacity separately and configure one or more publishers
as named WLO worker sources. WLO only polls their provider-neutral registry
outputs; it does not create, retain, resize, or tear down capacity and does not
send a workload request to a provider.

Inspect an unbound v0.3 plan without a profile, worker configuration or live
registry:

```bash
bin/wlo plan PLAN.json --workdir /absolute/path/to/workload
```

Run the plan against the configured local, remote or mixed registry set:

```bash
bin/wlo run PLAN.json \
  --workdir /absolute/path/to/workload \
  --output /absolute/path/to/output \
  --worker-sources-config /absolute/path/to/worker-sources.yml
```

See [named worker sources](worker-sources.md) and the checked-in single-source
and multi-source examples. WLO invokes each configured argv directly without a
shell on every poll. Each stdout must be one complete
`dynamic-worker-registry/v0.1` snapshot with a unique `registry_id`. Nonzero
exit, malformed output, duplicate namespaces, stale/replayed revision or
invalid registry data halts dispatch; there is no legacy fallback. The older
single-command flags remain available only as a one-source compatibility path.

No `--execution-profile`, `--workers-config`, `--paid-budget`,
`--authorize-paid-rpof` or workload-dispatch `--rpof-executable` option is
used for this path. WLO selects a compatible READY worker, persists its exact
identity, launches the workload command locally and injects its endpoint as
`WLO_WORKER_ENDPOINT`.

## Detached execution

```bash
bin/wlo start PLAN.json \
  --workdir WORKDIR \
  --output OUTPUT \
  --worker-sources-config /absolute/path/to/worker-sources.yml
```

The forked manager retains the immutable command argv and continues polling
after the initiating CLI process exits. For static v0.1 execution, supply
`--workers-config`. For logical v0.2 plans, also supply
`--execution-profile PROFILE.json`.
Paths containing spaces work when quoted normally. `start` detaches into a new
Unix session with stdin disconnected and stdout/stderr redirected to a manager
log. The launching terminal can close after the command returns. This feature
requires macOS or another Unix with Ruby `fork`; it does not prevent machine
sleep or resume a workload after reboot.

Success means a manager acquired the execution lock, validated the retained
execution identity, prepared state, and recorded its PID and log. Readiness
checks then run asynchronously. Use `summary` to distinguish work in progress,
readiness failure and final completion. Invalid inputs, changed execution
identity or a competing runner return an error before a launch is acknowledged.

Each launch writes `manager/UUID.log` and `manager/UUID.json` under OUTPUT.
The launch record contains PID, start/finish times, status, exit status and any
manager error. `manager.json` points to the latest launch. Earlier launch records
and logs remain intact through resume. Environment values are not copied into
manager records. Job stdout/stderr remain under `runs/JOB_ID/` as before;
they become available after each command exits.

## Check progress and results

```bash
bin/wlo summary PLAN.json --output OUTPUT
bin/wlo status PLAN.json --output OUTPUT --human
bin/wlo status PLAN.json --output OUTPUT --json
```

For priority-pool plans, `summary` and `status --human` default to a compact
dashboard: one ASCII successful-completion bar per pool, in plan order, followed
by one `ALL` bar. Pool rows show the FO-04 reason through `RUN`, `READY`,
`WAIT`, `PAUSE`, `BLOCK`, `FAIL`, or `DONE`, plus compatible READY (`r`), busy
(`b`), idle (`i`), and failed-job (`f`) counts. The `ALL` row reports running,
failed, and terminal counts separately, so failed or interrupted work never
fills the success bar. The default width is 72 columns; `--width COLUMNS`
supports wider panes.

Use `--verbose` to retain the detailed human report with execution-lock
activity, complete/failed/running/pending/interrupted counts, job IDs, timing,
manager records, and evidence paths. JSON is unchanged and retains every job
and the complete FO-04 pool status model. Explicitly queued retries become
pending again, so terminal progress can decrease after retry authorization.

Plain `status` retains its JSON default and existing fields. Additive fields
include `counts`, `total`, `terminal`, `progress_percent`, `executor_active` and
`manager`. `pool_status` adds per-pool job counts, compatible READY/busy/idle
worker counts, state, and deterministic reason. Reporting only reads state and briefly probes the existing lock;
it neither repairs stale records nor authorizes retries. State/job files are
individually atomic snapshots, so a live report may straddle a job transition.
The recorded PID identifies a past launch; it alone does not prove liveness.
`executor_active` reports the execution lock, including foreground runners.
Older runs without manager records or latest-run timestamps remain readable.
Latest-run elapsed time excludes readiness checks and previous paused periods.

For a continuously refreshed consolidated view, use:

```bash
bin/wlo watch PLAN.json --output OUTPUT [--interval SECONDS] [--verbose] [--width COLUMNS]
```

The default interval is one second. Interactive terminals redraw the stable
pool-plus-overall dashboard; redirected output appends the same plain snapshots
with separators and no cursor control. `--verbose` retains the detailed watch
view. The watcher exits automatically for `completed`, `workload_failed`, and
`interrupted` executions, and Ctrl-C exits the watcher without interrupting the
execution.

`watch` reads `execution.json`, `jobs.json`, current running-attempt metadata,
the pause sentinel, the execution lock, optional manager records, and all last
accepted per-source checkpoints (with legacy single-checkpoint fallback). It
does not instantiate a runner or poller, contact a publisher, claim work, or
write execution state. Busy workers are
matched to running attempts by their complete execution identity, including
generation and capability fingerprint. New checkpoints retain the validated
DW-19 worker snapshot so the existing scheduler can derive compatible capacity;
legacy checkpoints remain readable and are labeled when exact eligibility is
unavailable.

## Foreground cancellation

During `run` or foreground `resume`, SIGINT cancels only that WLO execution.
Every job is launched in its own process group, so WLO can signal the direct
child and descendants without matching process names or signalling the parent
shell. The first Ctrl-C stops dispatch and sends TERM; after a one-second grace
interval WLO sends KILL only to surviving owned groups. A second Ctrl-C requests
that escalation immediately. WLO waits for its direct children and output
readers before returning exit status 130. SIGTERM follows the same scoped path
and returns 143.

Cancelled attempts become terminal `interrupted` attempts. Metadata retains
the attempt, worker binding, request time, signal, owned PID/process-group ID,
TERM-versus-KILL outcome, exit information, and partial stdout/stderr. Ordinary
resume fails closed while interrupted attempts remain. Review their possible
side effects, authorize them with `retry-failed --job ... --reason ...`, then
resume. Completed attempts and never-dispatched pending jobs are preserved.

This guarantee applies when SIGINT/SIGTERM reaches the foreground WLO process.
It does not cover SIGKILL, power or kernel failure, or terminal loss that sends
no signal. Cancellation never contacts a publisher or provider and never
changes provider capacity.

## Pause, resume and retry

```bash
bin/wlo pause --output OUTPUT
bin/wlo summary PLAN.json --output OUTPUT
```

Pause requests stop further dispatch and allow active jobs to finish. Wait for
`running=0` and `executor inactive` before changing the checkout or restarting.
Then resume the same execution in the background:

```bash
bin/wlo start PLAN.json \
  --resume \
  --workdir WORKDIR \
  --output OUTPUT \
  --worker-sources-config /absolute/path/to/worker-sources.yml
```

Supply the dynamic source configuration again when resuming v0.3; it is
external discovery configuration, not persisted provider authority. The
existing per-source registry checkpoints, execution identity and attempt
history remain in OUTPUT. Supply the
original profile for profiled legacy runs. `--resume` is explicit: ordinary
`start` honors a retained pause request. If the breaker is tripped,
acknowledge it using `--resume --acknowledge-circuit-breaker` only after
reviewing the cause. Existing `retry-failed` selects and archives failed or
interrupted attempts; a subsequent `start --resume` executes those queued
retries. Start never implicitly retries either state. Foreground `run` and
`resume` remain available with the same source options.

Use `retry-failed ... --dry-run --json` before authorization to inspect exact
job IDs, prior status/attempt, evidence hashes, archive destinations and breaker
state. `wlo recovery PLAN.json --workdir WORKDIR --output OUTPUT` reads the
durable action history. `wlo repair` is deliberately distinct: no safe generic
repair transformation exists today, so preview reports unsupported and mutation
fails closed without changing evidence.

For a legacy v0.2 RPOF profile, every run/start/resume also supplies the original
`--paid-budget FILE`, absolute `--rpof-executable FILE`, and
`--authorize-paid-rpof`. Resume evaluates only the retained Step-6 handoff and
same budget binding. It never provisions replacement capacity, resets the
deadline, or widens the cumulative cap. These options are compatibility-only
and are intentionally absent from normal help; production v0.3 operators manage
the RPOF campaign externally and give WLO a `WorkerSource`.

Manager exit code is 0 for completed/paused execution, 2 for workload/breaker
failure, and 1 for manager errors. Read the launch result to learn that code;
the original `start` command already returned after startup acknowledgement.
Abrupt termination can leave an unfinished manager record and running job
records. Detached start provides no crash recovery or job cancellation: inspect
any remaining job subprocesses before resuming. Use the normal `pause` command
for an orderly stop. Never infer safe resume merely from an old PID.

Keep an existing production run's plan, workdir and output identity. Deployment
of this feature belongs after that runner has exited or drained to a pause.
