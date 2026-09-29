# Architecture

## Purpose

WLO is a generic command-workload execution layer.

Its responsibility is to accept already-compiled execution intent and run it
safely across configured workers. It does not own the domain semantics that
produced a workload or interpret domain-specific results.

## Ownership boundary

Upstream workload systems such as AFW own:

- domain-specific selection and policy;
- compilation of domain intent into executable jobs;
- domain validation and result interpretation;
- domain provenance.

WLO owns:

- strict execution-plan validation;
- worker selection and label checks;
- zero-cost admission for configured local/fixed compatibility workers;
- provider-neutral dynamic worker selection and reconciliation;
- optional Ollama model/digest readiness checks;
- direct local command dispatch to selected dynamic endpoints;
- bounded pool concurrency;
- cross-process job claims;
- pause and resume;
- sticky terminal job states;
- audited retry authorization and failed-attempt archives;
- failure circuit breaking;
- immutable plan/output identity;
- stdout/stderr/metadata execution evidence; and
- execution-only status reporting; and
- immutable attempt-local worker identity and registry evidence.

RPOF owns RunPod fleet/resource mechanics, model/bootstrap readiness, tunnels,
leases and cost safeguards, provider scaling/replacement, campaign admission
and teardown. It publishes ready workers through the provider-neutral registry.
WLO reacts only to that registry and never repairs provider infrastructure.

Dynamic executions retain the same execution ownership while idle. See
[dynamic-polling.md](dynamic-polling.md) for bounded polling,
waiting-for-capacity semantics, and scheduler-loop stop and resume rules.

## Execution boundary

Jobs are opaque to WLO. Each job supplies a direct argument vector and optional
environment overrides. WLO does not infer behavior from command names, job IDs,
pool IDs, output content, or upstream naming conventions.

The execution plan does not contain absolute checkout or output paths. The
operator binds a trusted working directory and an output directory at run time.
The canonical working directory is recorded in execution state and must remain
identical when resuming the same output.

## Resume and failure behavior

`complete` and `failed` are terminal job states. Ordinary execution and resume
skip both. A process that disappears while a job is recorded as `running` does
not leave an authoritative cross-process lock: the kernel releases the `flock`,
allowing a later executor to recover that non-terminal job.

Each plan declares consecutive and total failure thresholds. When a threshold
is reached, no additional pending job is dispatched. Already-running jobs may
finish. Continuing pending work requires explicit breaker acknowledgement;
terminal failed jobs are still not rerun.

`retry-failed` is the explicit exception to failed-job terminality. It requires
an idle execution, validated selection and a reason; a tripped breaker also
requires acknowledgement. It archives WLO-owned evidence and records all
selected attempts in one atomic execution-state update, leaving execution
paused. Retry authorization changes the effective job status to pending only
while the retained failed metadata still matches the authorized attempt number.
The next dispatch increments that number; the old authorization cannot retry a
later failure. Neither the frozen plan nor execution identity changes.

Run/resume hold an execution-wide nonblocking filesystem lock for their full
lifetime. Retry takes the same lock before validating or copying evidence, in
addition to the state lock. Existing per-job claims remain in use. The process
lock is released by the OS on exit. This serializes executors for one output;
configured worker concurrency within an executor is unchanged.

## Capacity boundary

The v0.3 production runtime has no paid-capacity authority. RPOF independently
enforces cumulative spend, runtime deadlines, leases, admission and guardian
cleanup. WLO may pause or stop its execution, but those actions do not create,
retain or destroy provider resources. An empty or incompatible registry is the
normal DW-16 waiting state.

The earlier WLO-owned paid-budget and fulfillment runtime remains isolated for
v0.2 rollback compatibility and is loaded only when an old RPOF execution
profile is explicitly used. See [legacy-rpof-compatibility.md](legacy-rpof-compatibility.md).



## Runtime placement overlay

The production provider-neutral handoff is a v0.3 logical plan plus a
`WorkerSource`. AFW emits no provider or resource identity. WLO binds each
attempt to a validated registry worker and freezes that identity and snapshot
as execution evidence. The v0.2 execution-profile overlay remains compatibility
only; see [execution-profile-v0.1.md](execution-profile-v0.1.md).
