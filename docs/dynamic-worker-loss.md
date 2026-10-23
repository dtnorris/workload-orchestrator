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

`DynamicWorkerLossReconciler` reads only the checkpoint durably accepted for
the running attempt's exact `registry_id`. Loss or replacement in one source
cannot affect a same-named worker in another registry namespace.

| Accepted registry observation | Running-attempt result |
| --- | --- |
| Full execution identity is present | Continue; readiness alone does not end the attempt. |
| `worker_id` is absent | `worker_disappeared` in-doubt failure. |
| Same `worker_id`, different `generation_id` | `worker_generation_replaced` in-doubt failure. |
| Same worker and generation, different endpoint | `worker_endpoint_changed` in-doubt failure. |
| Same worker, generation, and endpoint, different fingerprint | `worker_capability_changed` in-doubt failure. |
| Bound source's last accepted snapshot expires | `worker_source_expired` in-doubt failure. |

Endpoint equality never establishes continuity. A replacement generation at a
reused endpoint still ends the old attempt in doubt. The replacement is a new
worker that is eligible only for future work.

A failed refresh alone does not establish worker loss while the bound source's
last accepted snapshot is fresh. Expiry does establish that WLO can no longer
reuse the capacity, and reconciliation applies only to attempts bound to that
snapshot's `registry_id`. A same-named worker in another registry cannot
satisfy continuity.

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

## Reviewed acknowledgement without retry

`acknowledge-worker-loss PLAN.json REVIEW.json --workdir DIR --output DIR
--review-sha256 SHA256 [--dry-run]` acknowledges one stopped dynamic-worker-loss
halt and its currently tripped breaker. It never starts work, changes a failed
attempt to pending/complete, imports a result, clears pauses, or authorizes retry.
The original pending jobs remain eligible only for a later explicit resume.

The finite `wlo-worker-loss-review/v0.1` object names
`action: acknowledge_loss_without_retry`, nonblank `operator` and `reason`, the
exact plan ID/hash/workdir, stopped `execution_sha256` and `jobs_sha256`, the
existing WLO tree digest `workdir_evidence_sha256` of the whole stopped workdir (the review itself must live outside that tree), the
halt's `job_id` and `attempt`, an `attempts` mapping of every existing job to its
metadata hash, and `preserved_files` / `preserved_directories` mappings of
workdir-relative evidence to exact hashes / sorted directory entries. Every
attempt must be included in preserved files. Review paths reject traversal and
symlinks. The exact bytes are limited to 1 MiB and must match the independently
reviewed digest passed on the command line. Duplicate JSON fields are rejected.

Under the existing execution-owner and state locks, WLO checks that no attempt
is running/interrupted or queued for retry, that the review covers all existing
attempts, and that exactly the named native bound loss explains the current
halt after excluding only earlier losses with independently valid retained
acknowledgements. Every earlier acknowledgement is revalidated from its raw
review bytes/digest, parsed copy, plan/workdir/output binding and preserved
files/directories; the exemption requires the exact job, attempt and current
metadata hash. A matching metadata hash alone is insufficient. Missing, changed
or wrong-identity history leaves that loss unreviewed and blocks the action.
One atomic, fsynced execution-state replacement retains the exact review
bytes, parsed review, review hash, timestamp and prior halt/breaker under
`worker_loss_acknowledgements`, removes only that halt, resets the breaker to a
new generation, and marks execution pending. Jobs, requests, plan, attempts,
logs, receipts, prior breaker history and late evidence are untouched. The
control action's JSON output and `execution.json` are the acknowledgement audit;
the existing `recovery` command continues to report retry history separately.

Preparation/action-check revalidate the pinned evidence. Halt restoration skips
only that byte-identical acknowledged attempt. A changed review/evidence, another
loss, new failed attempt or new tripped breaker gains no clearance from the old
action. Repeating identical authorization is idempotent and never clears a later
halt/breaker. Future explicitly retried acknowledged jobs would change pinned
metadata and are intentionally outside this acknowledgement contract.

For sequential losses in one execution, retain each acknowledgement and its
reviewed evidence. Prepare a new review for the currently halted job only after
its late evidence is stable, preview with `--dry-run`, and apply with the same
independently reviewed bytes/digest. B's review must still cover all existing
attempts, including A. Review each current halt separately; acknowledgement of A
cannot clear B's halt or breaker. Once B is acknowledged, use a separate explicit
resume only after the ordinary identity, capacity, pause and recovery gates are
satisfied. Failed/in-doubt attempts remain failed, and original pending work is
the only newly eligible work. This grants no late-result adoption or retry.

The offline sequential-loss process proof is
`bundle exec ruby test/acceptance/fo_wlo_05.rb`. Run it separately from the
ordinary suite after required host checkpoint registration. It uses disposable
synthetic jobs, fresh processes and native public controls, and retains its
binary logs/evidence under the printed temporary root. It never accesses an
original execution or contacts an inference endpoint.

## Source observation journal

Before accepting a validated registry candidate, each poller durably retains its
exact stdout, the command source's exact binary stderr, and a correlation receipt
under its checkpoint directory's private `observations/` directory. The receipt
includes registry ID, revision, snapshot and stream hashes, worker count, time and
`phase: validated_before_checkpoint`. That phase never claims the later checkpoint
write succeeded. Use the matching checkpoint/reconciliation evidence to establish
acceptance. Journal files/directories are exclusively created, fsynced and renamed;
IO/durability failure refuses acceptance and follows existing failed-poll/expiry
rules. No old journal publication is replayed. Successful empty publications keep
publisher diagnostics across later READY snapshots. Sources with no diagnostic
channel retain an empty stderr; WLO does not invent a rejecting predicate.

Registry schema, identity, monotonic revision, freshness, worker matching,
capability validation, loss reconciliation, source authority and breaker policy
are unchanged. The journal has no automatic rotation; preserve it as evidence.
