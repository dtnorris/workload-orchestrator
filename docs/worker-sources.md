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
