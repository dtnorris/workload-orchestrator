# Operator diagnostics

`bin/wlo doctor PLAN.json JOB --output DIR` is a read-only explanation over
the retained execution report, FO-04 pool status, job metadata, registry
checkpoint, controls, breaker, manager, and FO-06 interruption evidence. Add
`--json` for the complete structured `wlo-execution-diagnostic/v0.1` result.

Dynamic execution reports include a `worker_sources` array. Each row names the
source, its required/optional policy, last accepted registry identity and
revision, publication and expiry timestamps, current fresh/stale/unavailable
state, last poll result, blocking status, and a concise failure reason. Human
`status --verbose` prints the same source health. An unavailable required
source is diagnosed as `REQUIRED_WORKER_SOURCE_UNAVAILABLE`; an unavailable
optional source remains visible without blocking healthy capacity.

`bin/wlo logs PLAN.json JOB --output DIR --lines 15` shows bounded retained
stderr/stdout tails plus metadata. The default is 15 lines and the maximum is
200. Environment assignments whose names end in a bounded secret suffix such
as `API_KEY`, `SECRET_KEY`, `ACCESS_TOKEN`, or `PASSWORD`, plus HTTP bearer
authorization values, are redacted. Ordinary variables such as `TOKEN_COUNT`
and `PUBLIC_API_URL` are retained; arbitrary-secret detection is not claimed.

Job IDs are opaque. Doctor and logs accept the exact canonical ID from the
supplied plan and do not infer domain-specific aliases or parse identifier
structure. A missing ID fails closed. A workload-specific front door may
resolve its own human shorthand before invoking WLO with the canonical ID.

Stages are `dependency_waiting`, `worker_discovery`, `registry_validation`,
`worker_eligibility`, `worker_capacity`, `paused`, `circuit_breaker`,
`dispatch`, `command_launch`, `command_execution`, `command_failure`,
`command_exception`, `foreground_cancellation`, `worker_generation`,
`owner_process`, `healthy`, and `unknown`. An unhealthy diagnosis contains one
safe action identifier; a healthy diagnosis contains none.

Doctor and logs never dispatch, pause, resume, retry, acknowledge a breaker,
poll a provider, or alter paid capacity.
