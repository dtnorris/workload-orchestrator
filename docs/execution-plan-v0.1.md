# WLO Execution Plan v0.1

Contract version:

```text
wlo-execution-plan/v0.1
```

The exact serialized JSON bytes are immutable execution intent. WLO calculates
SHA-256 over those bytes and binds an output directory to the tuple:

```text
(plan_id, plan_sha256, canonical_workdir)
```

A different plan or working directory cannot resume into that output.

## Shape

```json
{
  "contract_version": "wlo-execution-plan/v0.1",
  "plan_id": "example-plan",
  "failure_policy": {
    "max_consecutive_failures": 2,
    "max_total_failures": 3
  },
  "pools": [
    {
      "pool_id": "local-pool",
      "worker_names": ["local"],
      "required_labels": ["local"],
      "max_concurrency": 1
    }
  ],
  "jobs": [
    {
      "job_id": "example-1",
      "pool_id": "local-pool",
      "group_id": "adventure-1",
      "argv": ["ruby", "-e", "puts 'hello'"],
      "env": {}
    }
  ]
}
```

Unknown plan, pool, requirement, or job fields fail validation.

## Plan ID and job IDs

IDs are opaque to WLO and must match:

```text
[A-Za-z0-9][A-Za-z0-9._-]{0,127}
```

WLO never derives execution semantics from an ID.

Job IDs must be unique within the plan. Pool IDs must also be unique.

## Failure policy

Both values are required positive integers.

After either threshold is reached, the circuit breaker stops new dispatch.
Already-active commands may finish. Resume requires explicit acknowledgement of
the tripped breaker before pending work can continue.

Acknowledgement resets the breaker counters, not terminal job results.

## Pools

Required pool fields:

- `pool_id`
- `worker_names` — non-empty logical worker-name array
- `max_concurrency` — positive integer no greater than `worker_names.length`

Optional pool fields:

- `required_labels` — array of worker labels, default `[]`
- `requirements` — capability requirements, default `{}`

### Ollama requirement

A pool may request exact Ollama readiness:

```json
{
  "requirements": {
    "ollama": {
      "model": "model-name:tag",
      "expected_digest": "<64 lowercase-or-uppercase hex characters>"
    }
  }
}
```

Every selected worker in that pool must be configured as `type: ollama`, expose
a `base_url`, report the requested model through `/api/tags`, and match the exact
expected digest before any job dispatch occurs.

The model name/digest is plan data; WLO does not maintain a model-alias registry.

## Jobs

Required job fields:

- `job_id`
- `pool_id`
- `argv`

Optional:

- `env`
- `group_id` — opaque scheduling group identifier

`argv` is a non-empty array of non-empty strings. WLO executes it directly; it
does not invoke an implicit shell.

If every job supplies `group_id`, WLO executes groups in first-seen job order
and completes each group before dispatching the next group. Pool ordering and
per-pool concurrency still apply within a group. This lets producers request
domain-appropriate progress (for example, complete one adventure before moving
to the next) without WLO deriving semantics from opaque job IDs.

A plan must either supply `group_id` for every job or omit it for every job.
Plans that omit it retain the v0.1 pool-major scheduling behavior.

`env` maps environment names to either a string or `null`.

Precedence is:

```text
WLO process environment
  -> worker job_env
  -> job env
```

A `null` value explicitly removes that variable from the child environment.
Execution evidence records the overridden environment keys, not their values.

## Worker configuration

Worker configuration is runtime/machine state and is not frozen into the plan.
A command worker may look like:

```yaml
workers:
  local:
    type: command
    labels: [local]
    hourly_rate_usd: 0.0
    job_env: {}
```

An Ollama-backed worker additionally supplies `base_url` and `type: ollama`.

For v0.1, every selected worker must have exactly zero configured hourly cost.

## Execution evidence

WLO creates:

```text
OUTPUT/
  plan.json
  execution.json
  jobs.json
  attempts/
    JOB_ID/
      attempt-N/
        metadata.json
        stdout.log
        stderr.log
  claims/
  control/
  runs/
    JOB_ID/
      metadata.json
      stdout.log
      stderr.log
```

`plan.json` is the exact frozen plan bytes.

Per-job metadata includes generic execution fields such as job ID, pool ID,
worker, status, attempt, timestamps, elapsed time, exit status, argv, and the
names of environment variables overridden for that child process.

WLO does not parse domain-specific result files.

`attempts/` is created only by explicit failed-job retry. It retains the prior
attempt's available WLO evidence; `runs/` remains the latest attempt. Retry
history and attempt-bound authorization are additive execution-state fields,
not changes to the frozen plan contract. `jobs.json` includes attempt numbers
and reports authorized retries as pending. See README for retry operation and
upgrade semantics.
