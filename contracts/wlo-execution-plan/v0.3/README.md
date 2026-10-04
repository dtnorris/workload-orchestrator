# WLO execution plan v0.3

Status: frozen WLO-owned public contract for `wlo-execution-plan/v0.3`.
WLO owns validation, scheduling and retained execution semantics. Producers own
their workload intent, arguments, provenance and interpretation of results.
Changing a field or scheduling rule requires a new contract version; unknown
fields fail closed.

The authoritative [valid and invalid corpus](../../../test/fixtures/afw-wlo-v0.3)
and its [SHA256SUMS](../../../test/fixtures/afw-wlo-v0.3/SHA256SUMS) remain at their
established paths. That historical directory name does not make the API
domain-specific. Consumer copies are non-authoritative conformance material;
no sibling checkout is needed to validate a plan.

## Version boundary and legacy behavior

The artifact's exact `contract_version` selects its semantics. WLO must never
rewrite, upgrade, or reinterpret stored or supplied v0.1 or v0.2 artifacts.

| Plan version | Grouped behavior | Ungrouped behavior |
| --- | --- | --- |
| `wlo-execution-plan/v0.1` | Historical group-major execution. A later group cannot dispatch until the earlier group is terminal. | Historical pool-major execution. |
| `wlo-execution-plan/v0.2` | Historical group-major execution with the frozen hard cross-group barrier. | Historical pool-major execution. |
| `wlo-execution-plan/v0.3` | `group_id` is priority and reporting metadata only. Groups are not barriers. | Work-conserving deterministic priority scheduling. |

Existing v0.1/v0.2 plan bytes, retained copies, execution state, imports, and
resumes keep those historical meanings permanently. A producer that wants
v0.3 semantics must emit a new v0.3 artifact; a consumer must not synthesize
one from an older artifact.

New dynamic executions use v0.3. v0.1/v0.2 remain explicit historical
compatibility versions.

## Plan wire shape

JSON object keys are case-sensitive. Required and optional fields below are
exhaustive; extra fields are errors. Arrays have at least one entry unless
otherwise stated. A present required field cannot be `null`. Identifiers
match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and are unique within their kind.

v0.3 retains the v0.2 meanings of `plan_id`, `failure_policy`, `pools`,
pool capability requirements, `jobs`, `argv`, and `env`. Placement is not in
this plan. Under the current production architecture it comes only from a
provider-neutral dynamic worker source; binding a v0.3 plan to the legacy
`wlo-execution-profile/v0.1` overlay is forbidden.

| Field | v0.3 meaning |
| --- | --- |
| `contract_version` | Required exact string `wlo-execution-plan/v0.3`. Every other value is either handled by its own frozen contract or rejected as unsupported. |
| `plan_id` | Required stable identifier chosen by the producer, not an output directory or provider handle. WLO binds it alongside the exact byte hash. |
| `failure_policy` | Required object with positive integer `max_consecutive_failures` and `max_total_failures`; optional distinct integer `non_operational_exit_statuses` in 1–255, default `[]`. |
| `pools` | Required ordered nonempty array of distinct logical `pool_id` identifiers. Each pool has required `pool_id`, optional `required_labels` (default `[]`), and optional `requirements` (default `{}`). Placement, worker names, concurrency, budget, and provider targets are forbidden. |
| `requirements.ollama` | Optional object with required nonempty `model` and exact 64-hex `expected_digest`. Optional positive integer `required_context_length`, literal `true` `require_fully_gpu_resident`, and nonempty `required_gpu_id` of at most 256 characters retain their v0.2 meanings. |
| `jobs` | Required ordered nonempty array. Every `job_id` is unique and references one declared `pool_id`. Each job has required nonempty string array `argv`, optional `env` (default `{}`), optional `group_id`, and optional `depends_on_job_ids` (default `[]`). |
| `group_id` | Optional identifier used only for deterministic priority and reporting. Either every job supplies one or no job does. It never creates a dependency or dispatch barrier in v0.3. |
| `depends_on_job_ids` | Optional array of zero or more unique job identifiers. Omission normalizes to `[]`. Each referenced job must appear earlier in the plan's `jobs` array. Self, duplicate, unknown, and forward references are errors. `depends_on_group_ids` does not exist. |
| `env` | Variable names match `[A-Za-z_][A-Za-z0-9_]*`; values are strings or `null`. Omission, explicit `null`, and `""` retain their distinct v0.2 meanings. |

Unknown fields at the plan, failure-policy, pool, requirements, Ollama
requirements, or job level are errors. Wrong scalar/array types, mixed grouped
and ungrouped jobs, duplicate identifiers, unsupported versions, and invalid
dependency references fail closed.

## Process environment and placement

WLO begins with the inherited process environment and applies each job's `env`
as an overlay: a string sets the exact value, null removes a variable, and an
omitted name is unchanged. WLO then injects the selected dynamic endpoint as
`WLO_WORKER_ENDPOINT`, after persisting the attempt's execution identity.
This placement value wins over parent and job environment values.

The `wlo-attempt-placement/v0.1` interface is documented in
[dynamic dispatch](../../../docs/dynamic-dispatch-boundary.md). Domain adapters
translate this generic placement input into their own runtime configuration;
WLO launches opaque commands and does not interpret their domain results.

## Dependencies and terminal state

A pending job is dependency-eligible only when every job named by its
`depends_on_job_ids` is currently terminal:

- `complete` satisfies a dependency;
- `failed` also satisfies a dependency, matching historical group-barrier
  sequencing rather than introducing success-conditioned behavior;
- `pending` and `running` do not satisfy a dependency.

Imported terminal outcomes count exactly like locally produced outcomes.
If explicit retry changes a failed prerequisite back to pending, a dependent
that is still pending becomes ineligible again until that prerequisite is
terminal. A dependent that is already running is not preempted, and an
already-terminal dependent is not implicitly rerun. v0.3 has no blocked or
skipped lifecycle states and no success-dependent dependency form.

## Deterministic priority

Priority tuples use zero-based integer ranks.

For a grouped plan, each job's priority key is:

```text
(group_first_seen_rank, position_within_group, job_id)
```

`group_first_seen_rank` is assigned by the first appearance of each
`group_id` in the plan's `jobs` array. `position_within_group` is the
job's order among jobs with that group ID.

For an ungrouped plan, each job's priority key is:

```text
(0, absolute_plan_position, job_id)
```

Tuples compare lexicographically in ascending order. Integer components compare
numerically; `job_id` compares by its UTF-8 byte sequence. The job ID is a
stable final tie-breaker even though valid array positions already distinguish
jobs.

## Work-conserving assignment

At each serialized scheduling decision, the scheduler considers pending,
dependency-eligible jobs and currently idle, available workers. It may skip an
earlier-priority job that has no currently compatible available worker and
assign an earlier-compatible lower-priority independent job. This is
work-conserving priority scheduling, not strict FIFO and not a group barrier.

Compatibility edges come from the applicable worker, profile, and capability
contracts. DW-12 owns the implementation of that compatibility graph; it may
not change the ordering objectives below.

For one scheduling decision, choose a matching using these objectives in order:

1. maximize assignment cardinality;
2. among maximum-cardinality matchings, select the lexicographically earliest
   ordered sequence of assigned job-priority keys;
3. among remaining ties, select the lexicographically earliest ordered sequence
   of `(job-priority-key, worker-key)` pairs.

Pair sequences are sorted by job-priority key before comparison. A static/fixed
worker key is its stable configured worker name. A dynamic worker key is the
complete DW-01 execution-binding tuple, compared component by component:

```text
(registry_id, worker_id, generation_id, endpoint, capability_fingerprint)
```

Each string component compares by its UTF-8 byte sequence. The tuple is not
replaced by display labels, provider order, registry array order, Ruby hash
order, or thread timing. A changed generation, endpoint, or capability
fingerprint therefore changes the dynamic worker key.

Assignment selection and durable claim/attempt persistence occur before
dispatch. Running jobs are never preempted merely because a higher-priority job
becomes eligible or a new worker appears.

## Binding, import, and resume

The v0.2 byte-binding principle remains in force: WLO binds the exact plan
bytes and absolute workdir into execution identity. A production v0.3 plan has
no execution profile or static selected-worker binding; its attempts retain
their exact dynamic worker identities instead. Historical v0.2 executions
continue to bind and retain their exact profile and selected-worker bytes.
Formatting-only changes to any applicable bound artifact produce a different
identity.

A resume uses the plan's original version and semantics. Dependency eligibility
is recomputed from durable current job state, including accepted terminal
imports and explicit retries, without rewriting the plan. Imported jobs must
still satisfy the independent `wlo-terminal-import/v0.1` identity and evidence
checks. Neither import nor resume upgrades v0.1/v0.2 semantics.

Existing v0.1/v0.2 artifacts retain their historical bytes and environment
behavior. Their former RPOF-dispatch path is retired and cannot run a workload.
v0.3 defines the local process overlay above and routes every dynamic job
through WLO.
