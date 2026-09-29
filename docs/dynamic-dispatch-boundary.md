# Dynamic execution and legacy dispatch boundary

Supported dynamic v0.3 execution consumes `WorkerSource` snapshots and uses the
scheduler-selected `RegistryWorker` to create a durable dynamic attempt. WLO
runs the job's opaque argv/env locally, injecting the endpoint from that exact
attempt binding into `AF_OLLAMA_BASE_URL`. Local stdout, stderr, exit status, or
exception determine the command result; DW-14 can separately retain an in-doubt
worker-loss outcome. Neither path constructs or parses an RPOF dispatch envelope.

## Inventory and disposition

| Surface | Classification | Disposition |
| --- | --- | --- |
| `DynamicScheduler`, `RegistryWorker`, capability matcher | Supported dynamic path | Provider-neutral selection unchanged; no provider request/result schema. |
| `record_dynamic_running!`, dynamic attempt terminal state | Supported dynamic path | Binding/snapshot and local outcomes unchanged; no provider job/request fields. |
| `Runner#execute_dynamic_command` and dynamic result callback | Supported dynamic path | Local executor only; exact persisted endpoint, no legacy translation. |
| `remote_request`, remote execution/results/logs/evidence, provider command validation | Old static RPOF path | Already isolated by DW-22 in `LegacyRpofRunner`; retained for explicit historical v0.2 RPOF profiles. The factory rejects v0.3 plans or a `WorkerSource` before loading it, and direct legacy construction rejects both. |
| `RpofClient` dispatch process/JSON transport | Legacy/static compatibility | Deferred by the primary loader; direct API use or historical profile execution loads it explicitly. Frozen wire formats and safeguards remain. |
| RPOF dispatch request/job/summary validators | Legacy/static compatibility | Request/job validation moved to `Legacy::RpofDispatchContract`; response validation loads only on explicit compatibility calls. Public request/result methods retain their signatures. |
| Profile target validation and capability/readiness helpers | Legacy schema/read compatibility | Retained; loading them does not load workload dispatch validation or client transport. |
| Capacity, original budgets, guardian, admission and cleanup | Legacy/static compatibility | Already isolated by DW-22. Original deadlines, budget checks, admission and cleanup safeguards remain unchanged. |
| Historical `provider-attempt-*`, provider results, remote-in-doubt evidence and retry archives | Historical persisted state | Existing readers and explicit static compatibility behavior retained; no output migration or rewriting. |
| `remote_runner`, `rpof_client`, and contract tests/fixtures | Compatibility tests | Retained, including result translation, transport failures, original deadline, and retry safety. |
| RPOF execution documentation and CLI options | Documentation/compatibility | Marked compatibility-only; dynamic execution requires no remote request generation or provider job submission. |

Generic WLO dispatch, dispatch halts, job claims, and scheduler terminology
remain. These describe execution control, not provider workload dispatch.

## Loader policy

`require "workload_orchestrator"` loads the dynamic execution/reporting path
without loading the RPOF client, provider dispatch validator, compatibility
translator, or capacity implementation. Existing explicit provider APIs remain
available through Ruby autoloads. The historical static factory explicitly loads
`LegacyRpofRunner`; it does not add remote methods to every runner. Dynamic plans
and worker sources cannot select that factory branch, even with a legacy profile.
Dispatch contract calls retain thin compatibility entry points which load their
validator only when invoked.

DW-20 live progress and DW-21 read-only watch continue to read the same durable
execution and registry evidence. They do not load or call the legacy runner.

## Validation and deferred cleanup

Focused tests use provider-call traps during heterogeneous local execution,
zero-capacity polling, and generation replacement. A fresh subprocess also
runs a real local command with RPOF client construction forbidden and checks
that no legacy dispatch/provider implementation was loaded.

DW-33 owns final removal of historical static dispatch APIs, wire schemas,
fixtures, documentation, and old-state compatibility. DW-22 already isolated
provider/cost lifecycle behavior in the legacy runner. This change does not
remove safeguards from that retained historical path, add another remote
provider abstraction, or change scheduling,
retry, polling, attempt identity, or evidence semantics.
