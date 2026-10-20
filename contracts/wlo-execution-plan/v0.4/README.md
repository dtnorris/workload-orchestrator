# WLO execution plan v0.4

WLO owns this public contract, `wlo-execution-plan/v0.4`. It extends the
[v0.3 contract](../v0.3/README.md) solely with explicit minimum observed context
eligibility and duplicate JSON key rejection at every object level. All v0.3
wire fields, scheduling, dependency, environment, placement, byte-binding and
recovery rules apply unless stated here. Unknown fields remain errors.

## Context selection

A producer must emit a new artifact with exact `contract_version`
`wlo-execution-plan/v0.4` to use `pools[].requirements.ollama.minimum_context_length`.
It is an optional positive JSON integer. It cannot appear together with
`required_context_length`, even when their values are equal. Strings, fractions,
booleans, null, zero and negative values fail ingestion. There is no context
mode or alias field; such fields are unknown and rejected.

`required_context_length` retains exact equality in every supported version.
Omitting both context fields retains v0.3's unconstrained context behavior.
All other requirements remain independent: exact model/digest and requested GPU
identity, full GPU residency, labels, readiness and an unexpired accepted source.

| Requirement | Observed 131072 | Observed 262144 |
| --- | --- | --- |
| Exact `required_context_length: 131072` | eligible | ineligible |
| Minimum `minimum_context_length: 131072` | eligible | eligible |
| Minimum `minimum_context_length: 262144` | ineligible | eligible |

Only scheduling eligibility changes. A context is the selected model's actual
observed runtime `context_length` under the unchanged
[registry contract](../../dynamic-worker-registry/v0.1/README.md), never a model
maximum or assumed qualification setting. Publishers remain responsible for
truthful observations. A larger eligible context provides no evidence of
scoring quality or output compatibility and grants no scorer configuration or
qualification-policy change.

## Immutable execution and compatibility

v0.1/v0.2/v0.3 plan readers, field allowlists and exact context semantics remain
unchanged. The standalone
[ollama-capability-request/v0.1](../../ollama-capability-request/v0.1/README.md)
also remains unchanged: it requests exact capacity, refuses minimum fields,
and retains its canonical normalized bytes and semantic fingerprints. A new
standalone request contract is unnecessary for this scheduling-only extension.
Registry bytes and capability fingerprint inputs are unchanged.

The frozen v0.3 corpus includes `invalid/bad-version.json` using v0.4. Its bytes
and checksums remain unchanged: it is outside v0.3, while the expanded public
reader now recognizes it under this contract. Unsupported versions still fail.

Exact original plan bytes/hash remain the workload intent and execution binding.
Ingest, import, resume and explicit retry must never upgrade a frozen artifact
or replace exact requirements with minimum requirements. Historical scoring,
qualification, provenance, attempts and successful/failed outcomes are retained.
Existing Batch39b has no dependency on this opt-in and receives no migration,
retry or resumption authority from it.

Before dispatch, the existing immutable `worker_snapshot` retains the actual
selected context within `capabilities.ollama.models`, together with
`registry_id`, `worker_id`, `generation_id`, `endpoint`,
`capability_fingerprint`, `registry_revision` and `registry_snapshot_sha256`.
The retained plan separately identifies the workload minimum. A selected 262144
context therefore stays distinguishable from a 131072 minimum.

The [worker-loss rules](../../../docs/dynamic-worker-loss.md) remain binding.
Readiness-only updates with unchanged execution identity do not end an active
attempt. Changed context/fingerprint, generation or endpoint, disappearance and
source expiry retain in-doubt protection even when replacement capacity would
satisfy the same minimum. Recovery cannot erase historical loss. Only explicit
retry may create another attempt, retaining original evidence and binding new
dispatch to currently eligible capacity; no silent retry or rebinding occurs.
