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
| `run`, `resume` | FOREGROUND WORK OWNER | No WLO manager is detached. A terminated runner can leave non-terminal attempt evidence requiring inspection. | Ctrl-C interrupts the foreground runner. On the production dynamic path it stops new dispatch, waits for already-launched job threads, and records interruption; it is not the supported graceful-pause command. Terminal loss is not a cancellation or paid-safety guarantee. | Use `pause --output OUTPUT` for deliberate graceful pause. Use RPOF separately for paid teardown. |
| `start` | DETACHED WORK LAUNCHER | The manager owns execution and continues in its own Unix session after the initiating CLI or terminal exits. | Ctrl-C after startup acknowledgement affects only the shell, not the manager. Abrupt manager death has no crash recovery or child-cancellation guarantee. | Use `pause` for WLO work. For a production campaign, use `rpof campaign stop` for capacity. |
| `watch` | READ-ONLY OBSERVER | Any foreground or detached WLO runner, jobs, RPOF guardian, tunnels and provider resources continue unchanged. | Ctrl-C closes only the view. It does not pause/cancel work and does not tear down capacity. | Use `pause` or the applicable RPOF teardown command. |
| `status`, `summary` | ONE-SHOT INSPECTION | All existing execution and provider processes/resources continue. | Interrupting the request has no lifecycle meaning. | Use `pause` or RPOF teardown explicitly. |
| `pause` | CONTROL REQUEST | Running jobs and their runner remain until the existing pause contract drains them; pending jobs stay retained. | Ctrl-C after the request is durable does not strengthen it. The command never signals RPOF resources. | Wait for `running=0` and `executor inactive`; use RPOF separately for paid teardown. |
| `retry-failed` | CONTROL REQUEST | Execution remains paused with selected failures queued. No runner is launched. | Interrupting the CLI is not execution cancellation or provider teardown. | Use `start --resume` or `resume` to run queued work. |
| `import-terminal` | CONTROL REQUEST | Imported terminal evidence remains; no command or runner is launched. | Interrupting the request does not stop other work or resources. | Use normal WLO run/resume and RPOF lifecycle commands separately. |
| `validate`, `plan`, `worker-check` | ONE-SHOT INSPECTION | No WLO execution owner is created. | Ctrl-C only interrupts the inspection/check. | Use `run`/`start` to execute; use RPOF to change capacity. |

The cross-repository rule is absolute: a WLO pause, runner exit, Ctrl-C, shell
exit or terminal loss is not an RPOF campaign stop. WLO never proves provider
absence. For campaign-owned paid capacity, request teardown with
`bin/rpof campaign stop ...`, then use campaign status until the budget is
`CLOSED` and provider absence is verified.

## Production v0.3 dynamic execution

Start the RPOF capacity campaign separately. WLO only polls its provider-neutral
registry output; it does not create, retain, resize or tear down capacity and
does not send a workload request to RPOF.

Inspect an unbound v0.3 plan without a profile, worker configuration or live
registry:

```bash
bin/wlo plan PLAN.json --workdir /absolute/path/to/af-workloads
```

Run the plan against the current RPOF registry:

```bash
bin/wlo run PLAN.json \
  --workdir /absolute/path/to/af-workloads \
  --output /absolute/path/to/output \
  --worker-source-command /absolute/path/to/runpod-ollama-fleet/bin/rpof \
  --worker-source-arg workers \
  --worker-source-arg=--json
```

`--worker-source-command` is the executable. Repeat
`--worker-source-arg` once for each argument; use the equals form when an
argument begins with a dash. WLO invokes the resulting argv directly without a
shell on every poll. Stdout must be one complete
`dynamic-worker-registry/v0.1` snapshot. Nonzero exit, malformed output,
stale/replayed revision or invalid registry data halts dispatch; there is no
legacy fallback.

No `--execution-profile`, `--workers-config`, `--paid-budget`,
`--authorize-paid-rpof` or workload-dispatch `--rpof-executable` option is
used for this path. WLO selects a compatible READY worker, persists its exact
identity, launches the scorer locally and injects its endpoint as
`AF_OLLAMA_BASE_URL`.

## Detached execution

```bash
bin/wlo start PLAN.json \
  --workdir WORKDIR \
  --output OUTPUT \
  --worker-source-command /absolute/path/to/runpod-ollama-fleet/bin/rpof \
  --worker-source-arg workers \
  --worker-source-arg=--json
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

`summary` and `status --human` show execution status, execution-lock activity,
terminal-job count/percentage, complete/failed/running/pending counts, active and
failed job IDs with worker/attempt/exit code, pause/breaker information, latest
run timing, and the last manager PID/log/result location. Failed jobs count as
terminal, not successful. Explicitly queued retries become pending again, so
terminal progress can decrease after retry authorization. Lists are capped at
ten running and ten failed jobs; JSON retains all jobs.

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
bin/wlo watch PLAN.json --output OUTPUT [--interval SECONDS]
```

The default interval is one second. Interactive terminals redraw one compact
dashboard; redirected output receives plain periodic snapshots without cursor
control. The watcher exits automatically for `completed` and `workload_failed`
executions, and Ctrl-C exits the watcher without interrupting the execution.

`watch` reads `execution.json`, `jobs.json`, current running-attempt metadata,
the pause sentinel, the execution lock, optional manager records, and the last
accepted `dynamic-workers/checkpoint.json`. It does not instantiate a runner or
poller, contact RPOF, claim work, or write execution state. Busy workers are
matched to running attempts by their complete execution identity, including
generation and capability fingerprint. New checkpoints retain the validated
DW-19 worker snapshot so the existing scheduler can derive compatible capacity;
legacy checkpoints remain readable and are labeled when exact eligibility is
unavailable.

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
  --worker-source-command /absolute/path/to/runpod-ollama-fleet/bin/rpof \
  --worker-source-arg workers \
  --worker-source-arg=--json
```

Supply the dynamic source again when resuming v0.3; it is external discovery
configuration, not persisted provider authority. The existing registry
checkpoint, execution identity and attempt history remain in OUTPUT. Supply the
original profile for profiled legacy runs. `--resume` is explicit: ordinary
`start` honors a retained pause request. If the breaker is tripped,
acknowledge it using `--resume --acknowledge-circuit-breaker` only after
reviewing the cause. Existing `retry-failed` selects and archives failed
attempts; a subsequent `start --resume` executes those queued retries. Start
never implicitly retries a failed job. Foreground `run` and `resume` remain
available with the same source options.

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
