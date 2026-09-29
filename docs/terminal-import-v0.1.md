# Terminal handoff and failure classification

WLO accepts an explicit, zero-execution import of terminal jobs from another
system. The importer must first produce a WLO plan containing **every** job in
the handoff, including those that will remain pending. Only a fresh, unstarted
WLO output can accept its first import. The command checks the bound execution
identity and source evidence before writing state:

```bash
bin/wlo import-terminal PLAN.json HANDOFF.json \
  --workdir WORKDIR --output OUTPUT \
  --workers-config WORKERS.yml --execution-profile PROFILE.json
```

The profile is required for a logical v0.2 plan. The workers config is needed
for fixed workers; an all-RPOF profile does not require a local worker config.
This command neither checks provider readiness nor provisions or runs workers.

The handoff is UTF-8 JSON with exactly these fields:

```json
{
  "contract_version": "wlo-terminal-import/v0.1",
  "plan_id": "frozen-plan-id",
  "plan_sha256": "<SHA-256 of exact plan bytes>",
  "workdir": "/expanded/absolute/workdir",
  "execution_profile_sha256": "<SHA-256 of exact profile bytes, or null for v0.1>",
  "workers_sha256": "<WLO worker binding digest, or null for v0.1>",
  "jobs": [
    {
      "job_id": "previously-complete",
      "status": "complete",
      "exit_status": 0,
      "failure_class": null,
      "source_path": "output/previously-complete/metadata.json",
      "source_sha256": "<SHA-256 of the source evidence file>"
    },
    {
      "job_id": "previously-failed",
      "status": "failed",
      "exit_status": 42,
      "failure_class": "non_operational",
      "source_path": "output/previously-failed/metadata.json",
      "source_sha256": "<SHA-256 of the source evidence file>"
    }
  ]
}
```

The importing system chooses the job mapping and classification and must
validate its own source semantics before writing this handoff. WLO checks that
each source is a regular file inside the resolved workdir (at most 1 MiB),
matches its declared digest, and belongs to a unique plan job. It copies the
exact source bytes under `runs/JOB_ID/import-source`, the exact handoff under
`terminal-import.json`, and terminal metadata under `runs/JOB_ID/metadata.json`.
`execution.json` records the handoff digest, phase and imported count. A
different handoff, changed source, altered imported evidence or changed plan,
profile, worker binding or workdir fails closed. An interrupted import blocks
ordinary execution; repeat **the same** import to finish it. The WLO breaker
starts a new generation with zero counters; historical terminal calls remain
in `jobs.json` and the total/failed counts, but do not trigger a new breaker
before a new command runs.

`complete` requires exit status 0 and a null class. `failed` requires a
nonzero exit status (1–255) or null and class `operational` or
`non_operational`. Imported failures remain failed and terminal through
ordinary `run` or `resume`. A later **explicit** `retry-failed --reason` keeps
the imported metadata and evidence in the archived first attempt.

## Classifying new failed attempts

The optional plan failure policy key `non_operational_exit_statuses` is an
array of distinct process exit statuses from 1 through 255. It defaults to
`[]`. A failed local command or RPOF workload with one of those statuses is
recorded as `failure_class: non_operational`; other failures, including missing
exit statuses and transport failures, are `operational`.

Both kinds remain failed, terminal and counted toward `max_total_failures`.
An operational failure increments `consecutive_failures`; a non-operational
failure resets that consecutive counter to zero. Successful commands also
reset the consecutive counter. Neither classification changes explicit retry
authorization, remote dispatch halt, capacity management or paid budget
identity. Producers must reserve a distinct nonzero exit status for their
domain classification and ensure all other errors return ordinary statuses.
