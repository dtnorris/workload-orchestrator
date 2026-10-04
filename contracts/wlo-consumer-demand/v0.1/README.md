# wlo-consumer-demand/v0.1

Read-only public aggregate demand for one exact execution and pool:

```text
bin/wlo consumer-demand PLAN.json --workdir DIR --output DIR --pool ID
```

The equivalent public Ruby method is
`ExecutionStore#consumer_demand(pool_id:, now: Time.now.utc)`.
Queries do not prepare, repair, resume, retry, heartbeat, or modify execution evidence.
The query takes the existing shared state lock; contention, missing evidence,
or invalid evidence must not be interpreted as quiescence.

## Identity and observations

- `contract_version`: `wlo-consumer-demand/v0.1`.
- `consumer_id`: SHA-256 of JSON `[plan.sha256, absolute workdir, absolute output_dir]`.
  It distinguishes executions of the same plan in different output directories.
- `plan_sha256`, `pool_id`: exact plan and pool identity.
- `capability_fingerprint`: normative `ollama-capability-request/v0.1`
  semantic fingerprint for a complete capability requirement, or null for a
  non-capability pool. Incomplete historical capability requirements fail closed.
- `observed_at`: UTC query observation time, not a liveness heartbeat.
- `heartbeat_at`: retained consumer heartbeat, or null for historical state.
- `fresh`: heartbeat age is between zero and 30 seconds, inclusive.

The dynamic runner updates `consumer_heartbeat_at` during its polling loop.
Restart does not synthesize a heartbeat on read. Historical missing heartbeat
does not mean live demand. No provider lifecycle is owned here.

## Demand and quiescence

- `state`: `active`, `stale`, `missing`, `missing_heartbeat`, or retained terminal,
  paused, interrupted, or failure status. Unknown/busy evidence is conservative.
- `runnable_count`: selected pool's pending, dependency-eligible work, bounded
  by available pool concurrency where specified. It is zero unless execution
  is running with a fresh heartbeat and no pause, dispatch halt, interruption,
  or tripped breaker. Waiting for capacity is still active demand.
- `bound_count`: **whole-execution** running or uncertain attempts, not an
  assertion about a particular worker or a count of only the selected pool.
- `uncertain_count`: included in `bound_count`; worker-loss/remote-in-doubt,
  explicit unknown outcomes, and interrupted attempts count conservatively.
- `quiescent`: zero bound attempts and either fresh liveness or readable
  terminal/paused state. It does not authorize release by itself.

Current and archived attempts are inspected under WLO ownership. A retry or
nominal failed status does not clear remote uncertainty. Existing WLO semantics
retain late terminal evidence without resolving the original in-doubt marker;
this interface does not invent a resolution or discard that marker.

Missing source, heartbeat, or lock contention yields no usable proof. Invalid
identity/evidence fails the command. Clients must validate identity and freshness,
and must not use missing values as zero. No private attempt mapping is exposed.

A capacity owner must separately exclude the selected generation from future
placement, expire all earlier usable placement snapshots, then request a newer
observation establishing quiescence. Aggregate demand alone never proves that
a particular worker is idle.
