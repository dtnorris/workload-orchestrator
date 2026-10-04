# WLO execution measurements v0.1

`bin/wlo measurements PLAN.json --output DIR` emits the read-only
`wlo-execution-measurements/v0.1` JSON contract for one retained execution.
It never changes execution, retry, pause, breaker, scheduling, or capacity state.

The execution window begins at the retained first `started_at`. Its end is the
measurement clock for an active execution and the retained latest run finish for
a terminal or paused execution. Throughput is completed successful jobs divided
by that wall-clock interval. ETA is an estimate: remaining pending/running jobs
divided by measured successful wall-clock throughput. No successful samples
means ETA is unavailable; no remaining jobs means zero remaining time.

Command duration buckets retain successful, failed, interrupted, and currently
running samples separately. Utilization is deliberately narrow: the fraction of
the execution window in which at least one command attempt was observed active.
It is not worker-slot utilization and it makes no provider-cost claim.
The contract also exposes timed attempt intervals with only generic worker and
generation identity, allowing an explicitly associated presentation layer to
align owner-provided measurements without reading WLO private state.

Historical executions are read without rewrite. Missing or invalid timing is
counted and excluded. WLO does not retain reason-separated eligibility,
capacity, dependency, and pause intervals, so `queue_wait` is explicitly
unavailable rather than inferred.
