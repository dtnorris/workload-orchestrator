> Historical reference: DW-33 removed the WLO-owned RPOF execution path.
> Commands and executable adapters below are retired. Contract identifiers and
> persisted evidence remain valid for inspection and audit.

# WLO paid-budget contract v0.1

> Legacy v0.2 compatibility: production v0.3 capacity budgets and guardians are
> owned by RPOF. This WLO contract remains only for rollback until DW-33.

## Scope

This contract defines WLO's provider-neutral declaration and lifecycle plus a
process/JSON adapter to RPOF's independent guardian. The budget module itself
does not provision or dispatch; WLO's guarded RPOF runner composes it with
capacity fulfillment. Local/fixed paid-worker rejection and explicit RPOF
paid authorization remain in force.

`PaidBudget`, `PaidBudgetLifecycle`, and `RpofBudgetClient` are loaded by
`require "workload_orchestrator"`. No AFW or RPOF Ruby implementation is imported.

## Explicit declaration

Every key is required; unknown keys are rejected. Money and durations must be
positive finite JSON numbers. There are no default spending allowances.

| Field | Meaning |
| --- | --- |
| `contract_version` | `wlo-paid-budget/v0.1` |
| `budget_id` | Nonempty identity for one budget, retained across resume |
| `plan_sha256` | SHA-256 of exact execution-plan bytes, checked at construction |
| `expected_compute_usd` | Operator-supplied estimate, not a measured cost or permission to spend |
| `max_hourly_rate_usd` | Aggregate ceiling, including pending reservations and replacement overlap |
| `max_cumulative_compute_usd` | Parent compute liability cap, including teardown reserve |
| `max_runtime_seconds` | Original runtime lease; never refreshed on resume |
| `guardian_poll_seconds` | Independent guardian polling interval |
| `orchestrator_heartbeat_timeout_seconds` | Maximum tolerated WLO heartbeat age |
| `teardown_reserve_seconds` | Allowed provider deletion/absence verification window |

Construct with `PaidBudget.new(declaration, plan_bytes: exact_bytes,
execution_profile: profile)`. With an execution profile, its three declared
ceilings must agree exactly: `max_total_cost_usd` maps to
`max_cumulative_compute_usd`; hourly ceiling and runtime map directly. The exact
profile SHA is retained in the lifecycle binding. Omit the profile only for
standalone library use; the RPOF runner supplies it.

Let P = guardian polling seconds, H = heartbeat timeout, T = teardown reserve,
R = aggregate hourly ceiling, and C = cumulative compute cap:

- Require H >= 2P and runtime > P + H + T.
- Conservative additional compute after a WLO crash: R * (P + H + T) / 3600.
- Require expected compute + that crash reserve <= C.
- Latest modeled provider absence: original arm time + runtime + P + T.
- All derived quantities must remain finite, including arithmetic overflow.

`budget.bounds` exposes those quantities. They apply to budget-owned compute,
not unrelated resources, storage, account balances, or other provider charges.

## Existing independent enforcement is preserved

The adapter translates the declaration to
`afio-production-burst-budget/v0.1` and validates
`rpof-production-burst-budget-state/v0.1` responses. RPOF's existing wire contract
and guardian are unchanged. Expected cost and hourly ceiling remain WLO policy;
the existing provider contract carries the cumulative cap and runtime/heartbeat
limits. WLO checks reported aggregate rate, while fulfillment checks
requested rates and enforce the aggregate ceiling before each paid mutation.
A readiness check is not an atomic reservation.

RPOF owns the authoritative ledger, immutable limits, original deadline, durable
pre-mutation reservations, pending-resource liability, independent launchd
supervision, and deletion/absence verification. Its reservation formula includes
P + H + T, even if WLO crashes after provider creation but before recording the
returned resource ID. Scaling and replacements must use that same parent budget.

The finite loss model assumes the guardian host and launchd remain available,
provider deletion succeeds within T, and actual billing rates do not exceed the
reserved rates. It is not an unconditional guarantee against host loss or an
indefinite provider/API outage. RPOF retries failed teardown; this patch does not
claim that retries bound a provider outage. No real paid run is authorized or
validated by installing this patch.

## Lifecycle API and retained identity

Instantiate `PaidBudgetLifecycle.new(budget:, client:, binding_path:)`, where
`client` is `RpofBudgetClient.new(executable: absolute_rpof_path)`. The binding
path must remain inside the same execution's retained evidence on every resume.
Do not delete or change it to restart a lease.

| Method | Behavior |
| --- | --- |
| `start!` | Locks and persists the full declaration before arm; validates identity, limits, timestamps and guardian; sends initial heartbeat synchronously; returns bounds and original deadline |
| `check!` | Re-evaluates provider budget and guardian evidence; rejects inactive, expired, over-rate, or failed state |
| `finish!(reason:)` | Stops heartbeat, requests teardown, releases local ownership; returns false on failure and exposes `last_error` |
| `last_error` | Latched lifecycle/teardown failure, if any |

The same binding admits only one local lifecycle owner. Resume compares the full
WLO declaration and profile SHA, evaluates the existing provider ledger, and
checks the original deadline. It never calls arm again, resets accounting,
extends a deadline, or refreshes an already-stale heartbeat. A persisted arm
attempt with no readable provider ledger is refused; ambiguous outcomes require
inspection, not a fresh automatic arm.

The background loop evaluates readiness before every heartbeat. Any failure
latches, stops heartbeats, and requests teardown. Foreground `check!` also fails
closed. A timeout or malformed arm response is treated as potentially armed:
teardown is attempted even without a successful acknowledgement. If RPOF is
unreachable, heartbeat cessation and the original lease remain the independent
fallback. `finish!` must be called in an `ensure` by the owning lifecycle caller;
false means teardown was not acknowledged, not that resources are absent.

Budget subprocesses have a finite timeout (30 seconds by default), use literal
argv, and terminate/reap only their own command process group on timeout. The
launchd guardian is outside that process group. Guardian status is an existing
unversioned RPOF CLI response; WLO explicitly checks enabled/loaded, readiness,
independent PID, state, error, and fresh ledger heartbeat. Ledger responses must
match their version and full immutable budget identity/limits.

## Gates before enabling paid execution

Steps 6/8/10 must wire this lifecycle into fulfillment and unified execution,
retain the same binding across resume, atomically reserve every paid mutation in
RPOF, enforce pool and aggregate hourly ceilings including overlap, preserve the
original budget/deadline on scaling, and verify terminal cleanup. Installing
this library is not grounds to remove either zero-cost/RPOF execution block.
A subsequent paid pilot still needs explicit reviewed numbers, actual execution
root/HEAD/state verification, and evidence that the independent guardian works.

## Validation and source baseline

Inspected WLO `cb00e43c8a605f31db5652d9908da7e606445657`, AFW
`34b492577f251db09dca24bc0f164cc18d97fef0`, and RPOF
`2f2d094623888ef44a52a26b4454cc17f52fd9a4`.

`test/paid_budget_test.rb` checks declarations, crash arithmetic, profile
consistency, stale/future evidence, ownership, resume, deadline preservation,
uncertain arm outcomes, heartbeat failure, and teardown failure.
`test/rpof_budget_client_test.rb` checks wire translation, literal arguments,
invalid responses, and bounded subprocess cleanup. These tests use fakes and
harmless local executable fixtures; no API credentials, network, paid resources,
AFW checkout, RPOF checkout, or live launchd guardian are required. They do not
prove real provider deletion latency or live guardian survival.
