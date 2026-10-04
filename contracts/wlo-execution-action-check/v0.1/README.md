# Execution action check v0.1

`wlo action-check PLAN.json --workdir DIR --output DIR --action ACTION --json`
returns a read-only, point-in-time admissibility result. Actions are `run`,
`resume`, `retry-failed`, `recovery`, and `repair`. The contract is
`wlo-execution-action-check/v0.1`.

Every result contains `contract_version`, `action`, `disposition`, `reason`,
and `execution_status`. `execution_status` is the validated retained status,
or null when there is no retained execution, inspection is blocked by a lock,
or retained evidence cannot be validated. A `blocked` result also contains a
nonblank, generic human-readable `message`.

Dispositions:

- `execute`: the action may be submitted to its existing command, which still
  validates its arguments and current state. This is not retry authorization.
- `already_complete`: `run` or `resume` needs no execution; preserve evidence.
- `blocked`: do not delegate the action. Present `message` or use the stable
  reason identifier for application-specific presentation.

A successfully produced result exits 0, including `blocked`. Invalid CLI
arguments, invalid plans, or inability to load required configuration exit 1
and write an error to stderr. Human output is available by omitting `--json`.

| Reason | Disposition / meaning |
| --- | --- |
| `new_execution` | Execute a new `run`; output remains uncreated. |
| `no_retained_execution` | Block other actions without retained execution. |
| `retained_pending_execution` | Execute `run` or `resume` on ordinary pending state. |
| `paused` | Block `run`; it cannot clear or bypass pause. |
| `resume_paused_execution` | Explicit `resume` may be submitted; this check leaves pause intact. |
| `completed` | Already complete, for `run`/`resume` only. |
| `executor_active` | Block all actions while an executor owns the lock. |
| `state_busy` | Block while retained state is being updated; inspect again. |
| `circuit_breaker` | Block execution until explicitly reviewed acknowledgement. |
| `dispatch_halt` | Block execution on a retained halt or an unreconciled attempt loss. |
| `interrupted_attempts` | Block execution until explicit retry authorization. |
| `inactive_running_state` | Block execution on orphaned running state. |
| `running_attempts` | Block execution/retry while attempts remain running. |
| `workload_failed` | Block failed execution; explicit selected retry is required. |
| `recovery_required` | Block other retained failure/cleanup states. |
| `retained_execution` | Permit `recovery` history inspection of valid inactive evidence. |
| `retry_selection_required` | Retryable raw attempts exist and none is running. The retry command still owns job selection, reason, acknowledgement, archives, and idempotence. |
| `no_retryable_jobs` | Block retry when no failed/interrupted raw attempts exist. |
| `repair_unsupported` | Block repair, including callers intending a preview; no deterministic repair is supported. |
| `invalid_retained_execution` | Block malformed, incomplete, inconsistent, or mismatched evidence. |
| `execution_profile_not_runnable` | Block execution of a historical, non-runnable profile. |

The store validates the same frozen plan, workdir, profile/worker binding,
and terminal import identity used by its existing commands. Retained job
summaries must agree with the plan and authoritative attempt metadata.
Existing optional historical fields are not backfilled or rewritten. Legacy
bound plans may supply the existing `--execution-profile` and
`--workers-config` options; dynamic plans need neither.

Inspection opens only existing lock files read-only and holds nonblocking
shared locks during the snapshot. It does not create output or locks, rebuild
job summaries, reconcile attempts, repair evidence, clear pause, acknowledge
breakers, authorize retries, start an executor, or poll worker sources.

Admissibility is not a reservation. The eventual command must reacquire its
normal locks and revalidate current evidence. Readiness and action-specific
arguments are deliberately outside this check. In particular, an eligible
retry may still be refused by the real command for invalid selection,
missing acknowledgement, conflicting archives, or a dispatch halt that the
selected attempts cannot resolve.
