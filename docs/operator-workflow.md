# WLO operator runbook

WLO owns starting, observing, pausing, resuming and retrying generic workloads.
These commands use the same plan, execution lock, job claims and breaker as
foreground execution. Production v0.3 runs use a provider-neutral dynamic
worker registry. Historical provider-specific profiles remain inspectable but
cannot execute; their old paid/provider flags are removed.

Run from the exact chosen WLO checkout with its installed Ruby bundle. Set
`PLAN`, `WORKDIR`, `OUTPUT` and `SOURCES` to the original public plan, workload
root, execution output root and named-source configuration. Paths must remain
the same through inspection, pause and resume. Quote paths with spaces.
Plan commands are executable input: inspect the actual HEAD and working tree
before execution, and use only trusted frozen plans.

## Public inspection and ownership contracts

**Read-only / inspect:**

```bash
bin/wlo validate "$PLAN"
bin/wlo plan "$PLAN" --workdir "$WORKDIR"
bin/wlo action-check "$PLAN" --workdir "$WORKDIR" --output "$OUTPUT" --action run --json
bin/wlo measurements "$PLAN" --output "$OUTPUT"
bin/wlo consumer-demand "$PLAN" --workdir "$WORKDIR" --output "$OUTPUT" --pool "$POOL_ID"
```

Set `POOL_ID` to an exact plan pool. Measurements/demand require the appropriate
retained execution evidence; they do not create a runner or heartbeat.
Use [action check v0.1](../contracts/wlo-execution-action-check/v0.1/README.md)
for run/resume/retry/recovery/repair admissibility. Exit 0 can contain `blocked`;
inspect disposition/reason, not just command success. A check reserves nothing;
the real action revalidates under its normal locks.
Use [measurements v0.1](execution-measurements-v0.1.md) for measured execution
facts and [consumer demand v0.1](../contracts/wlo-consumer-demand/v0.1/README.md)
for generic demand/liveness. Missing/stale/uncertain consumer proof is not zero
bound work or release permission.

WLO owns the [v0.3 plan](../contracts/wlo-execution-plan/v0.3/README.md),
[capability request](../contracts/ollama-capability-request/v0.1/README.md),
[registry](../contracts/dynamic-worker-registry/v0.1/README.md) and
[named-source configuration](worker-sources.md). A publisher's public CLI
returns registry JSON; operators need no sibling implementation source.

**Retained-state/workload mutation:** run/start/resume, pause, retry-failed and
import-terminal change WLO-owned evidence/control. None authorizes paid
capacity. Retry with `--dry-run` and recovery/status/doctor/logs are inspection.

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
| `run`, `resume` | FOREGROUND WORK OWNER | No WLO manager is detached. | Ctrl-C stops new dispatch, wakes polling, sends TERM to each execution-owned job process group, escalates surviving groups to KILL after one second, reaps direct children, and retains partial output plus `interrupted` attempt evidence. A second Ctrl-C skips the remaining grace interval. | Use `pause --output "$OUTPUT"` for a graceful drain that never signals running jobs. Use provider tooling separately for capacity teardown. |
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
bin/wlo plan "$PLAN" --workdir "$WORKDIR"
```

Run the plan against the configured local, remote or mixed registry set:

```bash
bin/wlo run "$PLAN" \
  --workdir "$WORKDIR" \
  --output "$OUTPUT" \
  --worker-sources-config "$SOURCES"
```

See [named worker sources](worker-sources.md) and the checked-in single-source
and multi-source examples. WLO invokes each configured argv directly without a
shell on every poll. Each stdout must be one complete
`dynamic-worker-registry/v0.1` snapshot with a unique `registry_id`. Required
source failure blocks dispatch; optional source failure remains visible without
blocking healthy capacity. Namespace/revision conflicts fail closed; there is
no legacy fallback. See the named-source policy for exact classification. The older
single-command flags remain available only as a one-source compatibility path.

Current dynamic execution uses the named-source configuration rather than
legacy provider-specific execution flags. WLO selects a compatible READY worker, persists its exact
identity, launches the workload command locally and injects its endpoint as
`WLO_WORKER_ENDPOINT`.

## Detached execution

```bash
bin/wlo start "$PLAN" \
  --workdir "$WORKDIR" \
  --output "$OUTPUT" \
  --worker-sources-config "$SOURCES"
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

Use the returned launch identity and public status/summary for manager progress,
logs and final exit result. Preserve earlier execution evidence through resume;
do not inspect or edit private manager/attempt storage to infer safe recovery.

## Check progress and results

```bash
bin/wlo summary "$PLAN" --output "$OUTPUT"
bin/wlo status "$PLAN" --output "$OUTPUT" --human
bin/wlo status "$PLAN" --output "$OUTPUT" --json
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
bin/wlo watch "$PLAN" --output "$OUTPUT" --interval 1 --verbose --width 72
```

The default interval is one second. Interactive terminals redraw the stable
pool-plus-overall dashboard; redirected output appends the same plain snapshots
with separators and no cursor control. `--verbose` retains the detailed watch
view. The watcher exits automatically for `completed`, `workload_failed`, and
`interrupted` executions, and Ctrl-C exits the watcher without interrupting the
execution.

`watch` composes WLO-owned retained reporting without polling publishers,
claiming jobs or writing execution state. It preserves exact worker generation
and capability identity. Historical evidence remains readable and is labeled
when eligibility cannot be established. Use public status/doctor to inspect
source health; no private retained-file reading is needed.

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
bin/wlo pause --output "$OUTPUT"
bin/wlo summary "$PLAN" --output "$OUTPUT"
```

Pause requests stop further dispatch and allow active jobs to finish. Wait for
`running=0` and `executor inactive` before changing the checkout or restarting.
Then resume the same execution in the background:

```bash
bin/wlo start "$PLAN" \
  --resume \
  --workdir "$WORKDIR" \
  --output "$OUTPUT" \
  --worker-sources-config "$SOURCES"
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
state. `wlo recovery "$PLAN" --workdir "$WORKDIR" --output "$OUTPUT"` reads the
durable action history. `wlo repair` is deliberately distinct: no safe generic
repair transformation exists today, so preview reports unsupported and mutation
fails closed without changing evidence.

Historical provider-specific profiles cannot execute. Current dynamic capacity
is managed independently by the provider and discovered through named sources.
See [legacy compatibility](legacy-rpof-compatibility.md) for read-only history.

Manager exit code is 0 for completed/paused execution, 2 for workload/breaker
failure, and 1 for manager errors. Read the launch result to learn that code;
the original `start` command already returned after startup acknowledgement.
Abrupt termination can leave an unfinished manager record and running job
records. Detached start provides no crash recovery or job cancellation: inspect
any remaining job subprocesses before resuming. Use the normal `pause` command
for an orderly stop. Never infer safe resume merely from an old PID.

Keep an existing production run's plan, workdir and output identity. Deployment
of this feature belongs after that runner has exited or drained to a pause.

## Troubleshooting and stop points

Use `bin/wlo doctor "$PLAN" "$JOB_ID" --output "$OUTPUT" --json` and
`bin/wlo logs "$PLAN" "$JOB_ID" --output "$OUTPUT" --lines 15` for one
exact opaque canonical job. See [diagnostics](operator-diagnostics.md).
Admissibility, lock/identity conflict, interrupted work and breaker failures are
WLO-owned; inspect public status/action-check before explicit recovery.
Unavailable or mismatched registry evidence belongs first to the named publisher;
WLO reports the source failure and does not bootstrap or repair capacity.
Generic completion says nothing about domain acceptance. Stop on a refusal,
preserve evidence, and never edit retained state or delete evidence to force resume.
