# Architecture

## Purpose

WLO is a generic command-workload execution layer.

Its responsibility is to accept already-compiled execution intent and run it
safely across configured workers. It does not own the domain semantics that
produced a workload or interpret domain-specific results.

## Ownership boundary

Upstream workload producers own:

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
- execution-scoped job process groups and bounded foreground cancellation;
- cross-process job claims;
- pause and resume;
- sticky terminal job states;
- audited retry authorization and failed-attempt archives;
- failure circuit breaking;
- immutable plan/output identity;
- stdout/stderr/metadata execution evidence;
- execution-only status reporting;
- immutable attempt-local worker identity and registry evidence.

Worker publishers and provider tools own resource mechanics, readiness,
leases, cost safeguards, scaling/replacement, admission, and teardown. They
publish ready workers through the provider-neutral registry API that WLO owns.
WLO reacts only to registries and never repairs provider infrastructure.

One heterogeneous v0.3 plan becomes one WLO execution, scheduler and store.
Jobs from every logical pool coexist in that execution; WLO matches each
pending job to current compatible capacity. Dynamic executions retain the same
execution ownership while idle. See
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

`complete`, `failed` and `interrupted` are terminal job states. Ordinary
execution and resume skip all three. An interrupted execution requires explicit
retry authorization before resume. A process that disappears while a job is
recorded as `running` does not leave an authoritative cross-process lock: the kernel releases the `flock`,
allowing a later executor to recover that non-terminal job.

Each plan declares consecutive and total failure thresholds. When a threshold
is reached, no additional pending job is dispatched. Already-running jobs may
finish. Continuing pending work requires explicit breaker acknowledgement;
terminal failed jobs are still not rerun.

`retry-failed` is the explicit exception to failed/interrupted job terminality.
It requires an idle execution, validated selection and a reason; a tripped breaker also
requires acknowledgement. It archives WLO-owned evidence and records all
selected attempts in one atomic execution-state update, leaving execution
paused. Retry authorization changes the effective job status to pending only
while the retained failed metadata still matches the authorized attempt number.
The next dispatch increments that number; the old authorization cannot retry a
later failure. Each action has a deterministic ID and content hashes; an exact
repeat returns the existing authorization without another archive or audit row.
`retry-failed --dry-run` and `recovery` are read-only. Repair is a separate,
currently unsupported operation that validates identity and fails closed rather
than guessing at unknown state. Neither the frozen plan nor execution identity changes.

Run/resume hold an execution-wide nonblocking filesystem lock for their full
lifetime. Retry takes the same lock before validating or copying evidence, in
addition to the state lock. Existing per-job claims remain in use. The process
lock is released by the OS on exit. This serializes executors for one output;
configured worker concurrency within an executor is unchanged.

## Capacity boundary

The v0.3 runtime has no paid-capacity authority. Provider tooling independently
enforces any cumulative spend, runtime deadlines, leases, admission, and
cleanup. WLO may pause, stop, or crash, but those events do not create, retain,
or destroy provider resources and are not a paid-safety boundary. The
provider's original limits and cleanup authority remain active after WLO exits.
An empty or incompatible
registry is the normal `waiting_for_capacity` state: WLO keeps polling and
dispatches nothing.

The earlier WLO-owned paid-budget and fulfillment runtime remains isolated for
v0.2 rollback compatibility and is loaded only when an old RPOF execution
profile is explicitly used. See [legacy-rpof-compatibility.md](legacy-rpof-compatibility.md).



## Runtime placement overlay

The production provider-neutral handoff is a v0.3 logical plan plus one or more
named `WorkerSource` entries. Each publisher keeps its own `registry_id`,
revision sequence and durable checkpoint. WLO schedules across the union and
never rewrites those namespaces behind a synthetic front door. The workload
plan contains no provider or resource identity. WLO binds each attempt to a validated registry
worker and freezes that identity and snapshot as execution evidence. The
immutable identity is `registry_id`, `worker_id`, `generation_id`, `endpoint`
and `capability_fingerprint`. WLO starts from the
ordinary inherited process environment, applies the job environment, then
injects the selected attempt's `WLO_WORKER_ENDPOINT` last. Worker disappearance
or replacement fails the bound attempt in doubt and halts dispatch; a new
generation may receive only future work. The v0.2 execution-profile overlay
remains compatibility only; see
[execution-profile-v0.1.md](execution-profile-v0.1.md).
