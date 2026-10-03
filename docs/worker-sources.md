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
contract_version: wlo-worker-sources/v0.1
sources:
  - name: local
    command: /absolute/path/to/executable
    args:
      - workers
      - --json
    environment:
      EXAMPLE_VARIABLE: exact-source-specific-value
    workdir: /absolute/path/to/publisher
```

`sources` must be nonempty. Each row contains exactly the five fields shown.
Names must be unique and contain only lowercase letters, digits, underscores
or hyphens. `command` is an executable, `args` is an argv array,
`environment` is a string-to-string map (which may be empty), and `workdir`
must resolve to an existing directory. A relative `workdir` is resolved from
the configuration file's directory. Unknown keys fail closed. WLO invokes
every entry directly without a shell and without changing global `ENV`.

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

The legacy `--worker-source-command` plus repeated `--worker-source-arg` form
still configures exactly one source and retains
`OUTPUT/dynamic-workers/checkpoint.json` for compatible resume and reporting.
It cannot be combined with `--worker-sources-config`.
