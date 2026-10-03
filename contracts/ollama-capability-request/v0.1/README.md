# Ollama capability request v0.1

Status: public WLO runtime capability contract for
`ollama-capability-request/v0.1`.

`workload-orchestrator` owns this normative provider-neutral contract. It
describes only the exact Ollama runtime capability required by a workload. It
does not carry workload, plan, pool, provider, fleet, campaign, budget, lease,
worker, generation, endpoint, or other orchestration/provenance metadata.

The canonical valid fixture is
[`canonical-valid.json`](canonical-valid.json). Changing field meanings,
required fields, or canonicalization rules requires a new contract version.
Version v0.1 rejects unknown fields and duplicate JSON object keys.

## Document shape

The root object contains exactly:

| Field | Requirement |
| --- | --- |
| `contract_version` | Exact string `ollama-capability-request/v0.1`. |
| `ollama` | Exact capability object described below. |

The `ollama` object contains:

| Field | Requirement |
| --- | --- |
| `model` | Required nonempty, trimmed Ollama runtime model identity of at most 256 characters, with no control characters. Compared exactly and case-sensitively. |
| `expected_digest` | Required lowercase exact 64-hex model digest. |
| `required_context_length` | Required positive integer. Exact equality is required; a larger context is not a substitute. |
| `require_fully_gpu_resident` | Required boolean. Exact equality is required. |
| `required_gpu_id` | Optional nonempty, trimmed provider-neutral GPU identity of at most 256 characters, with no control characters. Compared exactly and case-sensitively when present. |

No other fields are permitted at either level. In particular, aliases and
external provenance cannot be attached to the request and are rejected rather
than ignored.

## Normalization and semantic fingerprint

The semantic fingerprint is lowercase SHA-256 over one canonical normalized
JSON value containing only runtime capability requirements. The normalized
value has one root key, `ollama`. Keys use this exact order:

1. `model`
2. `expected_digest`
3. `required_context_length`
4. `require_fully_gpu_resident`
5. `required_gpu_id`, only when present in the validated request

The normalized bytes are UTF-8 JSON generated without insignificant
whitespace and without a trailing newline. Input object key order and input
formatting do not affect normalization. Absence of `required_gpu_id`
deterministically omits that key; presence includes it in the fixed position
above. `contract_version` validates the wire document but is not a runtime
capability and therefore is not a fingerprint input.

For [`canonical-valid.json`](canonical-valid.json), the normalized bytes are:

```json
{"ollama":{"model":"qualified-model:latest","expected_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","required_context_length":131072,"require_fully_gpu_resident":true,"required_gpu_id":"NVIDIA A40"}}
```

Their fixed SHA-256 semantic fingerprint is:

```text
9121fe00d663bad2e5bd6f2ff4d6b492e66ba71b0843585c547321172dce5ae4
```

Two valid requests with identical runtime requirements have identical
normalized bytes and fingerprints regardless of their original key order or
formatting. Changing, adding, or removing any runtime requirement changes the
normalized bytes and fingerprint.

## Deliberate deferrals

This contract does not define worker advertising, registry semantics,
eligibility, scheduling, dispatch, provider lifecycle, preflight, bring-up,
budgets, leases, or provenance. Those concerns remain in their owning
interfaces. This version also does not migrate existing WLO execution-plan
requirements or any worker publisher.
