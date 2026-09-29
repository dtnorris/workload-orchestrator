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
- zero-cost admission for local/fixed workers and guarded RPOF paid capacity;
- optional Ollama model/digest readiness checks;
- direct local command dispatch and single-attempt RPOF dispatch;
- bounded pool concurrency;
- cross-process job claims;
- pause and resume;
- sticky terminal job states;
- audited retry authorization and failed-attempt archives;
- failure circuit breaking;
- immutable plan/output identity;
- stdout/stderr/metadata execution evidence; and
- execution-only status reporting;
- original paid-budget binding, capacity fulfillment and lifecycle control.

RPOF owns RunPod fleet/resource mechanics, model/bootstrap readiness, tunnels,
leases and cost safeguards, provider scaling/replacement and opaque dispatch.
WLO's paid execution calls those provider primitives within its own guarded
budget and lifecycle state.

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

## Cost boundary

Configured local/fixed workers remain zero-cost only. RPOF execution is a
separate guarded path: an immutable paid-budget document must match the exact
plan/profile bytes; RPOF independently enforces the cumulative cap, runtime
deadline, reservations and guardian cleanup; and WLO dispatches only within the
Step-6 capacity scope. Pause/retry resume never re-arms, re-fulfills or extends
that original authority.



## Runtime placement overlay

The provider-neutral handoff is now a v0.2 logical plan plus a separate WLO
execution profile. See [execution-profile-v0.1.md](execution-profile-v0.1.md).
AFW emits no worker names, local-only placement labels, or concurrency in new
plans. WLO binds placement at execution time and freezes that additional identity
for resume. RPOF profile validation is not authorization to provision capacity.
