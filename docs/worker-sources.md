# Named worker sources

WLO owns the frozen
[`dynamic-worker-registry/v0.1`](../contracts/dynamic-worker-registry/v0.1/README.md)
public provider API and its canonical valid and invalid fixtures. Independent
publishers implement that API. A publisher is not a workload front door and
does not need to know about any other publisher.

## Configuration

Production v0.3 executions pass a YAML configuration with
`--worker-sources-config FILE` (or `WLO_WORKER_SOURCES_CONFIG`). Its strict
shape is:

```yaml
contract_version: wlo-worker-sources/v0.2
sources:
  - name: local
    policy: required
    command: /absolute/path/to/executable
    args:
      - workers
      - --json
    environment:
      EXAMPLE_VARIABLE: exact-source-specific-value
    workdir: /absolute/path/to/publisher
```

`sources` must be nonempty. Each v0.2 row contains exactly the six fields shown.
Names must be unique and contain only lowercase letters, digits, underscores
or hyphens. `policy` must be `required` or `optional`. `command` is an
executable, `args` is an argv array,
`environment` is a string-to-string map (which may be empty), and `workdir`
must resolve to an existing directory. A relative `workdir` is resolved from
the configuration file's directory. Unknown keys fail closed. WLO invokes
every entry directly without a shell and without changing global `ENV`.

The previous `wlo-worker-sources/v0.1` contract remains accepted without
an ambiguous default: every v0.1 source is `required`, preserving its original
all-sources-required safety posture. New configurations should use v0.2 and
state policy explicitly. A required source blocks new dispatch only when it
has no fresh accepted snapshot. An optional source never blocks dispatch
merely because it is unavailable.

The source `name` identifies configuration and checkpoint storage only. It
does not replace, prefix or rewrite the publisher's `registry_id`. Publishers
must use distinct registry IDs; duplicates fail closed.

Examples cover [local-only](../examples/worker-sources/local-only.yml),
[remote-only](../examples/worker-sources/remote-only.yml), and
[mixed local-plus-remote](../examples/worker-sources/mixed.yml) configurations.
Replace their illustrative absolute paths before use.

## Scheduling and evidence

WLO polls each source independently, validates its snapshot against the public
contract, and stores its checkpoint at
`OUTPUT/dynamic-workers/sources/NAME/checkpoint.json`. It schedules across the
union of READY workers using the full registry-qualified execution identity:

1. `registry_id`
2. `worker_id`
3. `generation_id`
4. `endpoint`
5. `capability_fingerprint`

Consequently, identical `worker_id` strings from different registries remain
distinct workers. Attempt evidence retains the selected publisher's exact
registry ID, revision and snapshot hash. Reconciliation checks only that
publisher's later checkpoints, so disappearance, replacement, endpoint change
or capability change in one namespace cannot spill into another.

Every poll attempt also updates a source-local `health.json` beside the
checkpoint. This health evidence records policy, the last poll result and
failure reason. It is intentionally separate from `checkpoint.json`: a failed
command, malformed replacement, rollback or immutable-revision violation never
rewrites the last accepted checkpoint. `status --json` exposes each configured
source under `worker_sources`; human status prints the same policy, freshness,
poll result and blocking state.

An in-progress command is reported as `last_poll_result: polling`. Its retained
accepted checkpoint remains subject to the same freshness rules, but an
unfinished observation cannot establish a fully drained source result.

After a failed refresh, a source's last accepted snapshot remains usable only
until its own `expires_at`. WLO never extends that deadline. Expiry removes
only that registry's workers from scheduling. Expiry of a required source
blocks new dispatch until the same source accepts a valid continuation;
expiry of an optional source is nonblocking. Recovery resumes from the
source's retained identity and revision chain and automatically restores its
capacity.

The legacy `--worker-source-command` plus repeated `--worker-source-arg` form
still configures exactly one source and retains
`OUTPUT/dynamic-workers/checkpoint.json` for compatible resume and reporting.
It cannot be combined with `--worker-sources-config`.

## Voluntary offers and graceful source drain

These WLO controls affect **one named optional source in one existing v0.3
execution**. They do not change publisher readiness, qualification or provider
ownership. Ollama remains available for personal use. Other WLO executions and
clients are unaffected; control every relevant execution separately. WLO never
provisions remote capacity when a source is unavailable; eligible work waits.

The native runner registers its configured source authority in retained execution
metadata. Existing sources initially use explicitly labeled `legacy/unmanaged`
scheduling: qualified READY capacity remains eligible for compatibility, but is
never described as operator-offered. To deliberately adopt consent-based
scheduling on that running manager, withdraw, offer for two hours, inspect, and
later re-offer:

```sh
bin/wlo source-adopt --output OUTPUT --source local
bin/wlo source-status --output OUTPUT --source local
bin/wlo source-offer --output OUTPUT --source local --duration 7200
bin/wlo source-drain --output OUTPUT --source local
bin/wlo source-status --output OUTPUT --source local
bin/wlo source-offer --output OUTPUT --source local --duration 7200
```

Adoption immediately requests exclusion; it does not grant an offer. Once
adopted, no valid offer means excluded. It needs no manager restart, plan rewrite,
source configuration change or migration of a live installation. A manager
already executing older code must first be upgraded through the existing
operator-managed rollout; an old process cannot implement a new control boundary.
Sources without registered authority are rejected. Required sources cannot be
adopted, offered or drained: their existing required-source blocking policy is
preserved. Injected non-native command executors cannot claim the public start
boundary and therefore cannot accept source controls.

`--duration` is an integer from 1 through 604800 seconds. Offers perform no
discovery, bootstrap, model load, inference, workload launch or provider action.
Qualification and fresh registry evidence remain necessary. Offer consent uses
both the recorded UTC deadline and a monotonic deadline in the manager
incarnation that accepted the offer. Restart conservatively excludes retained
offers, even if their UTC deadline is in the future. Explicitly renew after a
restart; neither resume, retry nor status renews an offer. Wall-clock rollback
cannot extend the monotonic lease or revive it after restart.

Repeated adopt/drain is idempotent. Repeating an identical offer retains its
revision and deadlines, even after expiry or restart. Use `source-offer ...
--duration SECONDS --renew` to explicitly extend/change an existing offer.
Offering after drain creates a new revision. Re-offer never retries a failed or
in-doubt job and never clears a pause, breaker or dispatch halt.

### Acknowledgment and uncertainty

All four commands emit JSON. `requested` contains the durable intent and revision;
persisting that request alone is **not** scheduler acknowledgment. A runner
control thread acknowledges independently of publisher polling. `effective` and
`authoritative_boundary` require a matching revision/eligibility acknowledgment
from the current manager incarnation, a recent control heartbeat, its occupied
incarnation lock and an occupied execution lock. An unrelated status/recovery
lock holder cannot make an old acknowledgment current. A missing/dead/stale manager makes these false. Retained
acknowledgments are historical evidence, not a current safe-idle claim.

The authoritative boundary is actual subprocess start (`Process.spawn`). A job
selected before withdrawal remains pending until its claim is acquired and
eligibility is rechecked. The execution-local control lock serializes request,
acknowledgment and the interval from attempt binding through subprocess start.
A request racing an already binding launch waits until that subprocess has
started; its original attempt then finishes normally. No subprocess for an
excluded source can start after effective exclusion. A publisher delay/failure
cannot substitute for acknowledgment. A blocked claim or process-start operation
may delay acknowledgment; a persisted request must not be reported as effective
while waiting for that boundary.

`runtime_health` separately exposes publisher freshness/poll failures;
`offer_eligible` is null for unmanaged sources and boolean for managed sources.
`scheduling_eligible` describes consent only, not capability matching. Active
attempt counts come from retained attempt metadata, rather than aggregate demand.
`inference_running: true` means WLO has recorded active work; null means unknown,
including idle/dead-manager observations. It does not inspect arbitrary Ollama use.
`drain_result` is `draining` or `fully_drained` only with live effective exclusion,
fresh successfully polled publisher evidence and no in-doubt attempt. Otherwise
it is `uncertain`, even if recorded active count is zero. This is scoped only to
WLO-owned attempts. Process death or stale/malformed publisher evidence does not
prove safe idle.

Running records inherited by a new manager are also uncertain, including an
interruption between binding and subprocess start. A new heartbeat cannot turn
those records into proof that inference actually started or is still running.
Archived in-doubt attempts also keep the result uncertain; authorization of a
retry does not by itself establish that the old inference has stopped.

Ordinary status/summary adds `source_scheduling`, a read-only observation of
requested consent. Pool readiness keeps qualification counts separate from
`offer_excluded_idle` and reports `SOURCE_OFFER_EXCLUDED` when otherwise eligible
idle workers lack consent. These observations may straddle a control update;
only `source-status` reports the live authoritative acknowledgment boundary.

Polling and loss reconciliation continue for drained sources with their full
current workers/checkpoints. Disappearance, expiry and changes in generation,
endpoint, context, digest, residency or fingerprint retain the existing
[in-doubt and retry rules](dynamic-worker-loss.md). Consent never rewrites an
attempt identity or publisher fact. Frozen plan/configuration bytes stay intact.

Control metadata binds configured command/environment/workdir/policy authority
to the validated frozen execution identity and real output directory. Copied or
foreign outputs, changed configuration, replaced synchronization files, unsafe
paths and missing/corrupt managed records fail closed. An execution tombstone is
written before managed intent, so an interrupted adoption or deleted intent
cannot silently restore legacy scheduling. Such damaged authority requires
operator review; ordinary commands never infer a repair or permission to run.
