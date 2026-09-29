# Local operator workflow

WLO owns starting, observing, pausing, resuming and retrying generic workloads.
These commands use the same plan, execution profile, worker checks, zero-cost
policy, execution lock, job claims and breaker as foreground execution.

## Detached execution

```bash
bin/wlo start PLAN.json --workdir WORKDIR --output OUTPUT --workers-config WORKERS.yml
```

For logical v0.2 plans, also supply `--execution-profile PROFILE.json`.
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
`manager`. Reporting only reads state and briefly probes the existing lock;
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
bin/wlo start PLAN.json --resume --workdir WORKDIR --output OUTPUT --workers-config WORKERS.yml
```

Supply the original profile for profiled runs. `--resume` is explicit: ordinary
`start` honors a retained pause request. If the breaker is tripped, acknowledge
it using `--resume --acknowledge-circuit-breaker` only after reviewing the cause.
Existing `retry-failed` selects and archives failed attempts; a subsequent
`start --resume` executes those queued retries. Start never implicitly retries a
failed job. Foreground `run` and `resume` remain available.

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
