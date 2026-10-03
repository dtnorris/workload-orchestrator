# Operator diagnostics

`bin/wlo doctor PLAN.json JOB --output DIR` is a read-only explanation over
the retained execution report, FO-04 pool status, job metadata, registry
checkpoint, controls, breaker, manager, and FO-06 interruption evidence. Add
`--json` for the complete structured result.

Dynamic execution reports include a `worker_sources` array. Each row names the
source, its required/optional policy, last accepted registry identity and
revision, publication and expiry timestamps, current fresh/stale/unavailable
state, last poll result, blocking status, and a concise failure reason. Human
`status --verbose` prints the same source health. An unavailable required
source is diagnosed as `REQUIRED_WORKER_SOURCE_UNAVAILABLE`; an unavailable
optional source remains visible without blocking healthy capacity.

`bin/wlo logs PLAN.json JOB --output DIR --lines 15` shows bounded retained
stderr/stdout tails plus metadata. The default is 15 lines and the maximum is
200. Known RunPod, OpenAI, Anthropic, Google, AWS, and bearer-token forms are
redacted; arbitrary-secret detection is not claimed.

A canonical job ID ending in `-advNNNN-words` has the short form
`NNNN-<last-word>`. Full canonical IDs remain valid. Resolution is exact within
the supplied plan; a missing or ambiguous handle fails closed.

Stages are `dependency_waiting`, `worker_discovery`, `registry_validation`,
`worker_eligibility`, `worker_capacity`, `paused`, `circuit_breaker`,
`dispatch`, `command_launch`, `command_execution`, `command_failure`,
`command_exception`, `foreground_cancellation`, `worker_generation`,
`owner_process`, `healthy`, and `unknown`. An unhealthy diagnosis contains one
safe action identifier; a healthy diagnosis contains none.

Doctor and logs never dispatch, pause, resume, retry, acknowledge a breaker,
poll a provider, or alter paid capacity.
