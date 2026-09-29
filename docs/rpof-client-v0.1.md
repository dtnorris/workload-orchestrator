# WLO to RPOF client seam v0.1

## Scope and ownership

`WorkloadOrchestrator::RpofClient` owns the process/JSON boundary for capability
checks and opaque dispatch to an **existing** RPOF fleet. It calls an explicitly
configured absolute `bin/rpof` executable. It never loads AFW or RPOF Ruby code,
discovers sibling repositories, or routes through `bin/lme-rpof`.

AFW owns domain selection, compilation, provenance and interpretation. WLO owns
execution policy. RPOF owns provider mechanics, fleet state and resource safety.

This is migration step 3, a library interface for later WLO executors. It does
not add an alternate `wlo run` command, change the execution-plan schema, or
connect provider summaries to ExecutionStore. The current runner's zero-cost
gate remains in force. Do not use this transport as a production paid campaign
entry point: budget/guardian ownership, remote state integration and terminal
cleanup must land before that integration is enabled (steps 5, 8 and 10).

The base client has no fulfillment, budget, create, scale, admission or
shutdown methods. The separate `RpofBudgetClient` and `PaidBudgetLifecycle` add
[step 5 budget control](paid-budget-v0.1.md), without provisioning or enabling
paid execution. AFW's current callers remain until their replacements land.
Do not remove the old AFW client as part of this additive foundation patch.

## Frozen public versions and wire compatibility

All new callers use WLO names. There is exactly one explicit compatibility
adapter for the currently installed RPOF protocol; no version guessing or retry
with a weaker contract is allowed.

| WLO public contract | Existing RPOF wire contract |
| --- | --- |
| `wlo-rpof-capability-check-request/v0.1` | `afio-rpof-capability-check-request/v0.2` |
| `wlo-rpof-capability-check-result/v0.1` | `afio-rpof-capability-check-result/v0.1` |
| `wlo-rpof-dispatch-request/v0.1` | `afio-rpof-dispatch-request/v0.1` |
| `wlo-rpof-dispatch-summary/v0.1` | `afio-rpof-dispatch-summary/v0.1` |

Only `contract_version` is translated; the remaining fields retain their
meaning. Capability request v0.2 is intentional: it requires exact digests and
uses RPOF's runtime-alias provenance verification. Falling back to the old
digest-optional v0.1 request would weaken that check.

RPOF itself needs no update for this seam. Its persisted provider summaries
retain their original wire version. The returned WLO document has the public
WLO version. When RPOF adds native WLO wire versions, update this adapter and its
compatibility tests explicitly; do not silently accept arbitrary versions.

## Ruby API

Load `workload_orchestrator` (or `workload_orchestrator/rpof_client` directly),
then construct `RpofClient.new(executable: absolute_rpof_executable_path)`.

| Method | Input | Result |
| --- | --- | --- |
| `capability_check(request)` | WLO capability request hash | `RpofClient::Result` |
| `dispatch(request:, workdir:, output_dir:)` | WLO dispatch request; trusted local command working directory; new output directory | `RpofClient::Result` |

`Result` exposes `document`, `exit_status`, `stdout` and `stderr`. The provider
process may return 0 or 1 with a valid matching document. Not-ready capability
results and workload/infrastructure/drain failures are evidence, not transport
success. Inspect `document["ready"]` or `document["status"]`; do not treat the
mere presence of a Result as successful work.

Missing/invalid JSON, unsupported versions, inconsistent fleet/job evidence,
signals, exit 2 (rejected request) and other unexpected exits raise
`WorkloadOrchestrator::Error`. There are no automatic retries.

Calls block until the provider command exits, and capture its stdout/stderr.
This client adds no deadline, heartbeat loop, signal policy or remote cleanup.
Those belong to the future lifecycle owner. Provider resource safeguards are
not proof that WLO already owns a complete paid execution lifecycle.

## Capability request

Required fields:

- `contract_version`: the WLO capability request version above.
- `fleet_key`: RPOF logical fleet key, at most 64 identifier characters.
- `worker_selector`: `{"mode":"all"}` or
  `{"mode":"indices","indices":[1,2]}`; indices are unique positive integers.
- `requirements`: nonempty `models` array of `{name, expected_digest}` objects,
  positive integer `required_context_length`, and
  `require_fully_gpu_resident: true`. Optional `required_gpu_id` is a string.

Every digest is exactly 64 hexadecimal characters. Unknown request fields are
rejected. The result preserves provider diagnostics/capabilities. A ready result
must identify the requested fleet key, a nonempty fleet ID, and the selected
workers. Explicitly selected indices must match exactly.

Pool-to-requirement translation and readiness integration are step 4; this
interface takes an already-built request rather than interpreting a WLO plan.

## Opaque dispatch request

Required fields:

- `contract_version`: the WLO dispatch request version above.
- `target`: `{fleet_key, expected_fleet_id, worker_indices}`.
- `group_by_affinity`: boolean.
- `jobs`: nonempty array of `{job_id, argv}`, with optional `env` and `affinity`.

Jobs remain opaque. There is no AdventureFinder command-name inspection,
manifest parsing or score interpretation. Shell syntax in arguments remains
literal unless the job explicitly invokes a shell. `env` values must be strings:
WLO plan null/unset values cannot be represented by the existing RPOF protocol
and are rejected rather than silently dropped. Plan `pool_id`/`group_id` fields
are not wire fields; a future executor must deliberately construct its request.

`workdir` is canonicalized and must exist. `output_dir` must not already exist,
including as an empty directory or symlink. The client creates it atomically
before invoking RPOF and retains evidence on every outcome. This prevents a
prior summary from masquerading as a new result and intentionally does not
promise resume. Step 8 must define resume/attempt identity together with WLO
claims and ExecutionStore before relaxing this rule.

The summary must account for every requested job exactly once, agree with its
counts, and match the requested fleet ID/key. Provider statuses are preserved:
`completed`, `workload_failed`, `infrastructure_failed`, `integrity_failed`,
`drained`, `interrupted`. Corrupt evidence raises an error even if its status
already reports a failure. Only `completed` may accompany exit 0. A real
interruption exiting 130 raises a transport error; retained files remain for
inspection and future lifecycle handling.

## Verification and compatibility baseline

Source inspected for this seam:

- WLO `f41a4df0ab15b5001f8c0f27bff04c7892957344`.
- AFW `49f30f365a410901020f900ef92d788ce43092c3`.
- RPOF `2f2d094623888ef44a52a26b4454cc17f52fd9a4`, specifically
  `ContractV01`, `CapabilityCheck`, `DispatchV01`, `RunpodDispatcher` and the
  `rpof`/`rpof-capability-check`/`rpof-dispatch` entry points.

`test/rpof_client_test.rb` uses an external executable fixture, including
harmless local command dispatch. It tests wire translation, literal arguments,
environment forwarding, negative results, malformed evidence, invalid exits,
signals and stale-output refusal. It does not require AFW, a sibling RPOF
checkout, API credentials, network access, models or paid resources.

These tests verify the client boundary, not live fleet readiness, provider
cleanup or end-to-end paid execution.
