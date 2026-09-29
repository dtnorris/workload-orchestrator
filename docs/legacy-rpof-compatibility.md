# Legacy v0.2 RPOF compatibility

WLO's production v0.3 runtime consumes a provider-neutral `WorkerSource`. RPOF
independently owns campaign budget, deadline, guardian, scaling, replacement and
teardown, then publishes workers into the registry.

The earlier WLO-owned RPOF runtime remains temporarily available for historical
v0.2 execution profiles and rollback. It includes:

- `RpofClient`, `RpofBudgetClient` and `RpofCapacityClient`;
- `PaidBudget` and `PaidBudgetLifecycle`;
- `ExecutionPoolPlan` and `PoolFulfillment`;
- `WorkerAdmissionPolicy`;
- provider dispatch and resource-disposition compatibility fields; and
- the old `--rpof-executable`, `--paid-budget` and `--authorize-paid-rpof`
  options.

Legacy workload dispatch rejects v0.3 plans and dynamic worker sources. Request,
job and result validators load only through explicit historical dispatch APIs.
See [the dynamic dispatch boundary and inventory](dynamic-dispatch-boundary.md).

These classes are lazy-loaded only when a legacy RPOF profile or API is used.
Requiring `workload_orchestrator` and running a dynamic v0.3 plan does not load
them. Existing historical execution state remains readable, including provider
resource disposition. DW-33 owns final removal of this compatibility surface.

The compatibility contracts remain documented in:

- [paid-budget-v0.1.md](paid-budget-v0.1.md)
- [execution-pool-fulfillment-v0.1.md](execution-pool-fulfillment-v0.1.md)
- [rpof-client-v0.1.md](rpof-client-v0.1.md)
- [rpof-readiness.md](rpof-readiness.md)
- [rpof-execution-v0.1.md](rpof-execution-v0.1.md)
