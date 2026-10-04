# Dynamic execution boundary after DW-33

WLO consumes provider-neutral `WorkerSource` snapshots, persists the selected
`RegistryWorker` binding, and executes opaque jobs locally with that endpoint in
`WLO_WORKER_ENDPOINT`. Scheduling, polling, worker-loss evidence, retries, live
progress and watch behavior are unchanged.

The current placement interface is `wlo-attempt-placement/v0.1`. WLO persists
its `contract_version` and `endpoint_environment_variable` with each dynamic
attempt. The latter is `WLO_WORKER_ENDPOINT`, set to that attempt's bound worker
endpoint after applying the job environment overlay. A domain-owned command
adapter may translate it into its own configuration. WLO does not interpret
that adapter's domain or results.

The WLO-owned paid-capacity runner, clients, fulfillment, readiness and admission
implementations are removed. Historical provider-specific profiles are rejected by execution
and worker-check, while profile/plan validation and reporting remain available.

The lazy loader exposes only `RpofContract` and `PaidBudget` historical readers.
Dispatch validators load only for explicit historical contract inspection and
cannot execute commands. Persisted provider attempts, resource disposition and
remote-in-doubt evidence remain readable through the existing state/report code.
See [historical readability](legacy-rpof-compatibility.md).
