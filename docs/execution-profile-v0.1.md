# Provider-neutral execution profiles

Decision: separate workload intent from placement through a versioned runtime
JSON overlay. The plan remains the workload identity; the profile is an additional
execution identity. A profile cannot change jobs, argv, environment, group order,
failure policy, or model/digest requirements.

## Ownership and contracts

| Owner | Owns |
| --- | --- |
| Workload producer (e.g. AFW) | Logical pool IDs, exact requirements, opaque jobs, provenance |
| WLO profile | Backend selection, worker bindings, capacity, budget declarations |
| WLO runtime | Scheduling, state, resume identity; future fulfillment and budget lifecycle |
| RPOF | Future provider mechanics and independent resource guardian |

`wlo-execution-plan/v0.2` retains v0.1 top-level and job fields. Each pool contains
`pool_id`, optional `required_labels`, and optional `requirements` (the existing
exact Ollama model/digest shape). `worker_names` and `max_concurrency` are forbidden
in v0.2 plans. Hardware/provider placement labels belong in the profile; a plan's
labels are immutable capability requirements and cannot be removed by a profile.
The original plan bytes and SHA-256 remain unchanged when binding a profile.

`wlo-execution-profile/v0.1` contains `contract_version`, `pools`, and (only for
RPOF declarations) `budget`. Unknown fields are rejected. Every logical pool must
appear exactly once; extra, missing, and duplicate mappings are rejected.

## Local and fixed remote execution

```json
{
  "contract_version": "wlo-execution-profile/v0.1",
  "pools": [
    {
      "pool_id": "qwen",
      "backend": "local",
      "worker_names": ["mac"],
      "max_concurrency": 1
    }
  ]
}
```

Replace the pool IDs with those in the plan and map every pool. Local and
`fixed_remote` bindings use the same existing command runner: argv runs on the
WLO host, while worker configuration can direct inference to a local or an
already reachable remote Ollama endpoint. `fixed_remote` does **not** mean SSH,
remote command execution, file staging, or provisioning. Backend tags declare
operator placement intent; WLO does not infer locality from hostnames.

Both bindings require non-empty unique `worker_names`, a positive integer
`max_concurrency` no greater than the number of names, and optionally
`required_labels`. Profile labels add to plan labels. Workers are used in listed
order, up to `max_concurrency`; all listed workers must pass existing readiness
and zero-cost checks. Configure `base_url` and the job's endpoint environment
consistently in the worker file (for AFW, `AF_OLLAMA_BASE_URL`). A fixed remote worker
with positive declared hourly cost is still rejected.

To select a fixed remote endpoint, copy the profile, change `backend` to
`fixed_remote` and `worker_names` to the configured remote worker name. Reuse the
same plan and generated domain artifacts. Use a separate WLO output directory;
domain-owned result paths may still require separate domain execution workspaces
when running two copies concurrently. This feature does not isolate those paths.

```bash
bin/wlo validate /path/to/execution-plan.json --execution-profile /path/to/profile.json
bin/wlo plan /path/to/execution-plan.json \
  --execution-profile /path/to/profile.json \
  --workdir /path/to/af-workloads --workers-config config/workers.yml
```

`run`, `resume`, and `worker-check` accept the same `--execution-profile FILE`.
`run` and `resume` also require `--output DIR`. `validate` without a profile checks
only workload syntax. `plan` validates local bindings without inference or
readiness HTTP calls; `worker-check` performs readiness HTTP calls.
`status` and `pause` continue to use their existing interfaces.

## RPOF declaration and the #6/#8 interface

An RPOF pool replaces `worker_names` with `min_workers`, `desired_workers`, and
`max_hourly_rate_usd`. All counts are positive integers;
`min_workers <= desired_workers` and `max_concurrency <= desired_workers`.
`desired_workers` is the frozen capacity ceiling, not permission for unbounded
expansion. Its hourly ceiling covers the entire pool, not each worker.

A profile containing any RPOF pool requires one shared `budget` with positive,
finite numeric `max_hourly_rate_usd` and `max_total_cost_usd`, and a positive integer
`max_runtime_seconds`. All amounts are USD. Each pool's hourly ceiling must fit
the aggregate hourly ceiling; future admission must also enforce aggregate cost
across all simultaneously retained capacity. These are upper-bound declarations,
not a cost estimate, a guardian contract, or evidence of independent enforcement.

- #6 consumes the original logical requirements plus the profile's pool capacity
  and shared budget declaration. It owns fulfillment and resolved worker handles.
- #8 consumes those runtime handles and the unchanged opaque jobs, and records
  them in WLO's existing job/attempt/state model.
- #5 must define and prove independent cumulative-spend/runtime enforcement,
  heartbeat, teardown reserve, and budget/plan binding before any paid mutation.
- #9/#10 must inherit the same original budget/deadline through scaling, resume,
  pause, and shutdown. Re-reading the profile cannot reset a paid lease.

`validate` accepts this declared shape; `plan` prints it and explicitly reports
**BLOCKED**. Neither invokes RPOF nor claims readiness. `run`, `resume`, and
`worker-check` reject RPOF profiles before loading workers or creating execution
output. Even mixed local/RPOF profiles are rejected as a whole. This patch does
not lift the zero-cost gate, provision resources, or run a paid pilot.

## Resume and compatibility

New profile-backed runs retain `plan.json` and `execution-profile.json` verbatim.
Execution state records both SHA-256 identities and a fingerprint of the selected
workers' type, URL, labels, declared rate, and environment. Environment values are
hashed rather than copied into state. Worker-file reordering and unrelated workers
do not affect the fingerprint. Changes to any selected binding or profile bytes
(including JSON formatting) reject reuse of the output before clearing pause or
acknowledging a breaker. A changed placement is a new execution with a new output.

Existing v0.1 plans still require their original inline worker bindings and run
without a profile. Their stored state need not be migrated. Profiles cannot be
applied to v0.1 plans; frozen historical plans must not be rewritten to attach one.
Keep active Batch 039 on its original plan, workdir, and no-profile resume command.

Examples under `examples/profiles/` share `examples/logical-plan.json` and can be
checked with `validate`; local/fixed examples reference configured worker names.
The RPOF example is a schema fixture, not a recommended purchase or paid run.

## Failed-job retry

For a profiled execution, supply the original `--execution-profile FILE` and
`--workers-config FILE` to `retry-failed`, as with resume. Retry validates the
frozen profile and selected worker binding before archiving or authorizing any
attempt. Legacy v0.1 retries retain their existing no-profile path.
