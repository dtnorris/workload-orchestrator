# Execution-pool fulfillment v0.1

Step 6 adds an explicit paid-capacity library API. It joins an immutable logical
plan, an execution profile, and the step 5 budget. It does not dispatch jobs or
enable `wlo run`/`start` for RPOF profiles. Existing zero-cost runner gates remain.
AFW's legacy production path is retained until the unified execution migration.

## Ownership decision

| Owner | Data and behavior |
| --- | --- |
| AFW workload intent | Logical pool membership, opaque job argv, exact model/digest, required context and full GPU residency |
| WLO execution profile | Backend, concurrency, minimum/desired workers, pool hourly ceiling |
| WLO paid budget/lifecycle | Expected compute, aggregate hourly and cumulative ceilings, original runtime lease, heartbeat, guardian polling, teardown reserve, plan/profile binding |
| RPOF | Hardware selection, model acquisition, ready fleet handle, atomic budget reservations, provider mutations, independent guardian and deletion verification |

No domain names, scorer semantics, model registry, or AFW imports enter WLO.
No provider handle, worker count, rate, or budget enters the AFW logical plan.
The existing `wlo-execution-plan/v0.2` and `wlo-execution-profile/v0.1` shapes are
preserved, including step 4's logical capability fields. AFW's companion compiler
flags produce those fields explicitly for new manifests; old plans are unchanged.

All RPOF pools are simultaneously affordable: the sum of their hourly ceilings
must fit the aggregate ceiling. Minimum ready capacity is
`max(min_workers, max_concurrency)`; desired workers must already satisfy the
profile validator. There is no inferred spending allowance or oversubscription.
Missing model/digest, context or full-GPU requirement, arbitrary labels, a hard
GPU-ID constraint, and preselected fleet targets are rejected before provisioning.
The current fulfillment protocol cannot enforce those latter constraints.

## Library API

Offline inspection performs no provider calls:

```ruby
require "workload_orchestrator"
include WorkloadOrchestrator

plan = Plan.load("execution-plan.json")
profile = ExecutionProfile.load("execution-profile.json")
budget = PaidBudget.new(JSON.parse(File.read("approved-budget.json")),
                        plan_bytes: plan.bytes, execution_profile: profile)
pools = ExecutionPoolPlan.new(plan: plan, profile: profile, budget: budget)
puts JSON.pretty_generate(pools.preview)
```

After explicit approval of those actual finite budget numbers and provider
environment, an integrating caller can use the scoped paid API:

```ruby
client = RpofCapacityClient.new(executable: "/absolute/runpod-ollama-fleet/bin/rpof")
session = PoolFulfillment.new(pool_plan: pools, client: client,
                              output_dir: "/absolute/new-capacity-evidence")
session.with_capacity(authorize_paid: true) do |handoffs, lifecycle|
  # Capacity is live only in this block. Step 8 supplies the job executor.
  # Check lifecycle before each dispatch and stop dispatch on failure.
  lifecycle.check!
  consume_capacity(handoffs)
end
```

`authorize_paid` defaults to false and must be literal true. The block is required.
Installing the patch, running tests, inspecting `preview`, and calling
`plan_pool` do not authorize a paid invocation. The example callback is an
integration placeholder, not a provided dispatcher or command to run as-is.

The scope first dry-runs every pool and requires zero initial workers. It then
arms one shared parent budget, verifies the independent guardian, starts the
heartbeat, and fulfills pools sequentially under that same identity and original
deadline. Each returned pool undergoes a fresh step 4 readiness check, exact
worker ownership verification against the budget ledger, and an actual pool-rate
check. Pending reservations prevent a ready handoff. Partial ready capacity is
accepted only when it meets the effective minimum.

`RpofCapacityClient#fulfill_pool` also requires the active lifecycle to belong to
the exact same client, budget declaration and profile. Its subprocess timeout is
bounded by the parent's remaining runtime. Budget and capability subprocesses
have finite timeouts. The adapter invokes literal argv; no shell interpolation.

## Provider compatibility and retained evidence

The public request/result versions are
`wlo-rpof-execution-pool-fulfill-request/v0.1` and
`wlo-rpof-execution-pool-fulfill-result/v0.1`. The adapter translates to existing
RPOF `afio-rpof-execution-pool-fulfill-request/v0.2` and result `/v0.1`.
It passes the existing nested `afio-production-burst-budget/v0.1` unchanged in
meaning. RPOF's guardian contract needs no modifications.

Provider pool IDs are 16 hexadecimal SHA-256 characters derived from budget ID,
exact plan SHA, exact profile SHA and logical pool ID. This fits RPOF's current
handle truncation without collisions between ordinary logical pool name prefixes.
WLO requires the exact expected `ep-<provider-pool-id>-<plan-sha-prefix>` response;
a changed provider naming protocol fails closed. `pull_model` carries the same
runtime model identifier as `ollama_model`; RPOF's hardware/model registry still
owns the actual acquisition and runtime alias creation.

The fresh output directory retains `intent.json`, `budget-binding.json`, each
pool's preflight and fulfillment request/wire/result/stdout/stderr, fresh readiness
proof, `capacity.json`, and `session.json`. Capacity handoffs have version
`wlo-execution-pool-handoff/v0.1`, logical pool ID, plan/profile SHA, budget
identity, original deadline, actual hourly rate, exact fleet ID and worker indices,
requirements and returned capacity. A handoff is evidence, not a transferable
lease or permission to dispatch after the block exits.

There is no automatic capacity retry, adoption or resume. Existing output or
preexisting capacity is refused. Do not delete evidence, rotate budget IDs or
create another directory to bypass an uncertain prior outcome; inspect the
original ledger and guardian first. Lifecycle binding still preserves the
original deadline for callers using its separate resume API.

## Failure and cleanup

Success, exceptions, and interrupts leaving the scope request parent teardown.
A later-pool failure tears down the earlier pools too. An uncertain creation
response is treated as potentially spending. A teardown failure raises (or
preserves the original exception) and records the cleanup error; it never reports
verified resource absence. Background heartbeat failure stops heartbeats and
requests teardown. SIGKILL/host-process loss relies on the independent guardian,
original runtime lease and durable pre-mutation reservations, as specified in
[the paid-budget contract](paid-budget-v0.1.md). WLO never disables that guardian.

The existing guardian-host availability and provider deletion-window assumptions
still apply. This step does not implement terminal absence polling, remote job
dispatch or paid resume; those remain required for unified execution. Keep the
runner gates until that integration and an explicitly approved live pilot prove
the end-to-end behavior.

## Validation baseline

Built against WLO `ab744b7bcb53b794b2d3a41722204be82cedcdd9`, AFW
`34b492577f251db09dca24bc0f164cc18d97fef0`, and inspected RPOF
`2f2d094623888ef44a52a26b4454cc17f52fd9a4`.
Tests exercise the real adapter, budget lifecycle and readiness code with a fake
provider process boundary plus a harmless executable fixture. They cover exact
translation, aggregate ceilings, explicit authorization, stale evidence,
wrong identities, insufficient readiness, unowned workers, uncertain creation,
later-pool failure and failed teardown. They do not create paid resources or
prove live guardian survival/provider deletion latency.
