# Architecture

## Purpose

WLO is a generic command-workload execution layer.

Its responsibility is to accept already-compiled execution intent and run it
safely across configured workers. It does not own the domain semantics that
produced a workload or interpret domain-specific results.

## Ownership boundary

Upstream workload systems own:

- domain-specific selection and policy;
- compilation of domain intent into executable jobs;
- domain validation and result interpretation;
- domain provenance.

WLO owns:

- strict execution-plan validation;
- worker selection and label checks;
- zero-cost admission for v0.1;
- optional Ollama model/digest readiness checks;
- direct command dispatch;
- bounded pool concurrency;
- cross-process job claims;
- pause and resume;
- sticky terminal job states;
- audited retry authorization and failed-attempt archives;
- failure circuit breaking;
- immutable plan/output identity;
- stdout/stderr/metadata execution evidence; and
- execution-only status reporting.

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

WLO v0.1 is intentionally zero-cost only. Any selected worker whose configured
hourly rate is greater than zero is rejected before execution state is created.

Provider-specific paid-resource lifecycle, cumulative spend enforcement, and
remote resource creation are deliberately outside this milestone. A future paid
integration must add independently enforced finite cost/runtime controls rather
than weakening this boundary.
