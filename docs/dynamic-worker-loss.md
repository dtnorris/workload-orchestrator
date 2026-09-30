# Dynamic worker-loss semantics

Dynamic attempts belong to one exact `dynamic-worker-registry/v0.1` execution
identity:

1. `registry_id`
2. `worker_id`
3. `generation_id`
4. `endpoint`
5. `capability_fingerprint`

`ExecutionStore#record_dynamic_running!` writes that identity, plus the
accepted registry revision and snapshot SHA-256, before returning the dispatch
token. A dynamic dispatcher must retain that token and use
`record_dynamic_terminal!`; it must not use the legacy unbound attempt API.

## Reconciliation rules

`DynamicWorkerLossReconciler` reads only the checkpoint durably accepted by
`WorkerRegistryPoller`.

| Accepted registry observation | Running-attempt result |
| --- | --- |
| Full execution identity is present | Continue; readiness alone does not end the attempt. |
| `worker_id` is absent | `worker_disappeared` in-doubt failure. |
| Same `worker_id`, different `generation_id` | `worker_generation_replaced` in-doubt failure. |
| Same worker and generation, different endpoint | `worker_endpoint_changed` in-doubt failure. |
| Same worker, generation, and endpoint, different fingerprint | `worker_capability_changed` in-doubt failure. |

Endpoint equality never establishes continuity. A replacement generation at a
reused endpoint still ends the old attempt in doubt. The replacement is a new
worker that is eligible only for future work.

The in-doubt attempt remains terminal `failed` with `failure_class` set to
`non_operational`; the execution receives a `dynamic_worker_loss` dispatch halt
and reports `infrastructure_failed`. It is never changed back to pending by
ordinary polling or resume.

## Durable evidence

The attempt's `metadata.json` stores `dynamic_worker_loss_in_doubt` evidence
with the job and attempt IDs, exact execution identity, original registry
binding, last accepted registry identity/revision/hash, matching DW-10
reconciliation event, observation timestamp, loss reason, unknown-outcome
marker, and replacement identity when one is observed. The derived `jobs.json`
and human status output expose the loss reason; full evidence remains in the
attempt metadata.

The transition order is:

1. write failed/in-doubt attempt metadata and evidence;
2. write the execution dispatch halt;
3. rebuild derived job state;
4. return the persisted transition to the dynamic scheduler.

The scheduler may release occupancy only after the reconciler returns a
`recorded_in_doubt` transition. Resume repairs a dispatch halt if a crash
occurred after step 1 and before step 2.

## Races and retry

Attempt completion and loss use the same execution-store lock. A terminal
result durably recorded first wins, and later loss does nothing. Loss durably
recorded first wins; a later result is appended as `late_evidence` and cannot
rewrite the in-doubt outcome. A single snapshot that replaces generation A
with B records the DW-10 changed event containing both identities.

The existing explicit retry path archives the complete prior attempt before
authorizing a new one. A retried dynamic attempt gets a new attempt number and
must bind to a currently READY registry worker through
`record_dynamic_running!`. Prior loss and late-result evidence is never
overwritten.

The production dynamic scheduler uses this transition directly. It maintains
one compatibility graph and occupancy map for every logical pool in the single
execution. Occupancy is released only after the persisted reconciler
transition. A replacement generation can then be considered for future work,
but it can never make the prior attempt successful or automatically retry it.
