# Batch 039 execution interruption reconciliation

`script/reconcile-batch039-interruption.rb` is a one-time companion to the
reviewed workdir migration. Routine `wlo repair` remains unsupported. This
script accepts only the original pinned snapshot plus the exact reviewed
migration. It never retries a job, clears a breaker, launches WLO, calls a
provider, or changes the frozen plan. Migration alone still produces public
`resume` refusal with `interrupted_attempts`.

## Separate acceptance gates

Offline fixture proof does not authorize production changes. Before any real
mutation, the operator must separately approve it, prevent other launchers from
starting, verify the exact current production snapshot and AFW authority, and
freshly establish both source-host identity and remote process absence. The
old retained format contains no source-host UUID or complete child inventory;
those cannot be inferred from a manager PID, terminal record, or free lock.

The bounded local proof requires a reboot on the confirmed source host after
the retained TERM interruption. It reads the `IOPlatformUUID` from
`ioreg -rd1 -c IOPlatformExpertDevice`,
`sysctl -n kern.boottime`, and `ps -axo pid=`. All retained manager records must
be terminal by the interruption time, the stale manager pointer must agree,
and no historical manager PID may appear in the fresh inventory. PID reuse,
same-boot recovery, incomplete/contradictory ownership, unavailable facilities,
or uncertain host identity block recovery. No alternate process API or sandbox
bypass is used. A reboot proves only local termination; remote absence is a
separate explicit operator acceptance gate.

The operator creates a private (mode 0600), external verification JSON record
immediately before preview/apply, after performing those checks:

```json
{
  "contract_version": "wlo-batch039-operator-verification/v0.1",
  "snapshot_sha256": "3fc12311020429507851a2096e7f156f1401f524089fa72b6670c75be08a1969",
  "output": "/absolute/reviewed/output",
  "checked_at": "REPLACE_WITH_FRESH_UTC_ISO8601",
  "operator": "REPLACE_WITH_REVIEWER",
  "source_host_uuid": "REPLACE_WITH_VERIFIED_ORIGINAL_HOST_UUID",
  "source_host_confirmed": true,
  "remote_processes_absent": true,
  "remote_evidence": "REPLACE_WITH_REVIEWED_PROVIDER_AND_WORKER_ABSENCE_EVIDENCE_REFERENCE"
}
```

This record is human authority, not an automatically authenticated provider
report. Do not set its booleans based on offline results or historical files.
The operator must investigate all relevant retained worker bindings and remote
owners, including remote subprocesses independent of the old local manager.
An empty or unknown inventory cannot be accepted as absence. The script does
not contact providers or establish capacity readiness. The record is bound to
the exact output path and pinned snapshot, must be no more than 60 seconds old,
and is checked repeatedly under WLO's existing execution and state locks.
Refresh the review and record if it expires; never extend the limit.

## Public procedure after separate authorization

Use Ruby 4.0.6 in an authorized integration checkout containing both scripts.
Keep verification records and all before/after evidence outside source control.
Run the reviewed migration first on a separately verified snapshot. Then:

```sh
ruby script/reconcile-batch039-interruption.rb \
  --output "$REVIEWED_OUTPUT" --verification "$FRESH_VERIFICATION" \
  --reason 'Reviewed execution-only TERM reconciliation'
```

Preview is read-only. Review its exact evidence and obtain separate mutation
authorization. Freshly repeat the production liveness/remote checks, create a
new verification record, and repeat the command with `--apply` only after that
authorization. An apply flag and nonblank reason are mandatory for mutation.

Apply publishes a durable `control/pause`, then atomically replaces only
`execution.json`. The original TERM object moves unchanged into
`interruption_history`; status becomes `paused`. The embedded reconciliation
audit retains the exact prior execution serialization, its checksum, operator
record, liveness observation, reason and explicit-resume requirement. The
original migration audit and every retry/breaker/attempt/result record remain.
The operation does not rebuild `jobs.json` or declare the batch completed.

Verify the public interface after applying:

```sh
bin/wlo action-check "$REVIEWED_OUTPUT/plan.json" \
  --workdir /Users/davidnorris/code/adventure-finder-components/af-workloads-production \
  --output "$REVIEWED_OUTPUT" --action resume --json
```

Expected: `execute / resume_paused_execution`. Public `run` remains blocked by
the pause. Action-check success proves admissibility only; production resume,
capacity readiness, five-failure review and later scoring acceptance require
their own authorization and evidence. No run/resume command is part of this proof.

## Interrupted or repeated reconciliation

Exclusive creation, file fsync, atomic rename and directory fsync preserve a
safe interruption boundary. A crash before pause leaves the original snapshot.
A crash after pause but before rename leaves the original interruption plus
pause and possibly `execution.json.interruption-reconciliation.tmp`. The next
attempt refuses the changed snapshot; preserve those bytes for review. The
temporary file may contain a complete candidate audit, but is not a commit.

A crash after rename leaves the committed audit and pause. A final directory
fsync error is reported as an error, even if the rename is visible. Inspect the
actual state; do not infer success or roll back blindly. Repeated/partial
recovery always refuses, rather than overwriting evidence or claiming a new
success. After reviewing a partial precommit, restart from a newly verified
snapshot and repeat the whole public journey; automatic cleanup is unsupported.

Keep the original snapshot, migration evidence, operator review, preview/apply
results, hashes and final action-check output. Source integration and human
production authority remain independent of offline implementation acceptance.
