> Historical reference: DW-33 removed the WLO-owned RPOF execution path.
> Commands and executable adapters below are retired. Contract identifiers and
> persisted evidence remain valid for inspection and audit.

# WLO-owned RPOF execution v0.1

> Legacy v0.2 compatibility: production v0.3 commands run locally against the
> selected registry endpoint. This dispatch path remains only until DW-33.

Step 8 makes WLO the execution/state authority for RPOF-backed jobs without
creating a second remote scheduler. WLO claims one job, records its attempt as
running, selects one worker from the Step-6 handoff, and invokes RPOF with a
single-job request. RPOF remains the provider transport and resource-safety
owner; AFW remains the workload compiler and result interpreter.

## Admission

The operator supplies the exact logical plan, execution profile, paid-budget
document and absolute RPOF executable. `run`, `start` and `resume` require the
explicit `--authorize-paid-rpof` flag. Before any paid mutation, WLO validates
the plan/profile/budget hashes, job wire shapes, finite limits and aggregate
pool ceilings. The Step-6 scope dry-runs and fulfills capacity under the same
independently guarded budget.

## State and outcomes

WLO owns job claims, attempt numbers, `jobs.json`, stdout/stderr, progress,
breaker, pause and retry authorization. Provider evidence is retained under
`runs/JOB/provider-attempt-N/` and archived with the WLO attempt.

- A validated `completed` provider result completes the WLO attempt.
- A validated `workload_failed` result is a normal failed WLO attempt; other
  independent jobs may continue until the WLO breaker stops dispatch.
- Infrastructure, identity, integrity, timeout or transport failures make the
  current attempt failed/in-doubt, record an execution-wide dispatch halt, and
  stop new claims. Already-running attempts drain.
- A stale WLO `running` record after owner loss is converted to an in-doubt
  failed attempt. Ordinary resume never replays it. `retry-failed` is required.

## Pause, retry and capacity reuse

Pause stops new claims and lets active single-job RPOF calls finish. A paused or
otherwise retryable run stops its heartbeat and releases the local binding lock
without changing the provider budget. The independent guardian will tear down
capacity if another owner does not reconnect within the original heartbeat
window.

Resume reopens the same binding and evaluates the same ledger. It verifies the
unchanged deadline, handoff identity, fleet readiness, worker indices, rates and
budget ownership. It never invokes arm or fulfillment. Expired, stale, changed
or already-tearing-down state is rejected. Successful completion requests
teardown and records cleanup disposition; RPOF verifies provider absence.

`retry-failed` archives the complete WLO/provider attempt and clears an
infrastructure dispatch halt only when the selected retry includes the exact
attempt that caused it. The next resume increments the WLO attempt number and
uses a new provider evidence directory.

## Owner-death containment

Every dispatch receives an inherited owner-liveness pipe and a timeout bounded
by the original budget deadline. WLO owner death closes the pipe. RPOF then
interrupts its dispatcher, terminates active workload process groups and writes
interrupted evidence. If either process cannot complete cleanup, the independent
guardian, cumulative cap and original runtime lease remain in force.

Later guarded-capacity work added worker admission, scaling and terminal
lifecycle under the original budget. RPOF retains provider deletion verification.
The historical AFW execution bridge is outside WLO's current contract.
