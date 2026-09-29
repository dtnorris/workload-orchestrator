# RPOF pool readiness

`wlo worker-check` can validate already-existing RPOF capacity without AFW.
It translates the logical pool's requirements through WLO's existing process/JSON
client. Only the RPOF `capability-check` operation is invoked. WLO does not create,
bootstrap, resize, renew, dispatch to, or destroy a fleet during this operation.

## Declare the requirements and target

A v0.2 plan may add these fields to `requirements.ollama`:

| Field | Meaning |
| --- | --- |
| `model` | Exact runtime model name; existing required field |
| `expected_digest` | Exact 64-hex model digest; existing required field |
| `required_context_length` | Positive integer; exact context configuration, not a minimum |
| `require_fully_gpu_resident` | Must be `true` when specified |
| `required_gpu_id` | Optional exact GPU identifier string |

RPOF readiness requires the first four fields. WLO does not infer context or
residency from a model name, profile, worker config, or a domain-specific default.
A profile cannot override these requirements. Each pool still has one model;
WLO translates it to the provider contract's `models` array.

The matching RPOF profile pool may add a `target`:

```json
"target": {
  "fleet_key": "existing-fleet-key",
  "worker_selector": { "mode": "indices", "indices": [1] }
}
```

Alternatively use `"worker_selector": {"mode": "all"}` for all active workers.
Indices must be unique positive integers. A target is optional for declaration
and fulfillment, but mandatory for readiness against existing resources.
No active fleet is guessed. It resolves to the current fleet generation under
that key; the result reports the concrete `fleet_id` and selected indices.

The selected count must be at least both `min_workers` and `max_concurrency`, and
at most `desired_workers`. An explicit invalid count fails before invoking RPOF;
`all` is checked against the provider's returned selection. `desired_workers`
remains the capacity ceiling defined in step 7.

## Check existing capacity

```bash
bin/wlo worker-check /path/to/plan.json \
  --execution-profile /path/to/profile.json \
  --rpof-executable /absolute/path/to/runpod-ollama-fleet/bin/rpof
```

For a profile containing only RPOF pools, no `workers.yml` is needed. Mixed
profiles also accept `--workers-config FILE` and check their fixed workers using
the existing readiness and zero-cost rules. All declaration and fixed-binding
validation finishes before the first provider call or HTTP request.

`--rpof-executable` is an explicit absolute executable path, passed as argv without
a shell. This option is for `worker-check`; it does not enable execution.
The paired `examples/rpof-readiness-plan.json` and
`examples/profiles/rpof-readiness.json` illustrate the schema with placeholder
model/digest/fleet values. They are fixtures, not a paid run recommendation.

Success prints `PASS`, fleet identity, selected indices, and every provider
diagnostic, including `SKIP`. Failure returns a nonzero exit with pool-specific
reasons. WLO verifies the returned model, digest, context, residency, optional GPU,
selection, and capacity rather than accepting `ready: true` alone. The library
result retains the full provider document as `provider_result`.

RPOF v0.2 capability checks inspect its runtime-alias evidence for the current pod
generation, tunnel/endpoint health, and cost/shutdown gates. These checks do not
warm a model or perform scoring. Provider-state checking may be skipped when no
API key is available; that skip is shown in WLO output. Capability provenance is
RPOF's recorded evidence, not a new inference benchmark or a guarantee of future
availability.

## Compatibility and execution boundary

Existing model/digest-only local and fixed-remote checks are unchanged. The new
context/residency/GPU checks are implemented through RPOF. Selecting them on a
fixed endpoint fails explicitly; WLO does not silently ignore unsupported
requirements. RPOF also rejects arbitrary `required_labels`, which its capability
contract cannot verify. Legacy v0.1 plan syntax remains unchanged.

`validate` checks syntax and `plan` inspects the RPOF declaration without
acquiring paid capacity. Paid start requires WLO's guarded budget/capacity path.
A readiness pass alone neither enforces the profile's paid budget nor grants
permission to provision or dispatch. WLO's start/resume path separately requires
the original finite budget, guarded fulfillment, explicit authorization, and
RPOF's independent provider safeguards.

No changes to AFW or RPOF are required. The step 3 client translates the public
`wlo-rpof-capability-check-request/v0.1` into RPOF's existing exact-digest
`afio-rpof-capability-check-request/v0.2` wire contract.
