# Manual and scoped pause continuation

## Selecting a retained execution

From the chosen WLO checkout, the shortest handle invocation is:

```bash
bin/wlo pause --execution "$ID"
```

Use the full 64-character lowercase hexadecimal `pause_control.consumer_id`
from `bin/wlo status PLAN --output DIR --json`. This is the existing public
consumer identity: SHA-256 of the JSON array containing the frozen plan's exact
byte hash, original absolute workdir and absolute output path. It is public,
not an owner bearer token. A plan ID and a manager launch ID are different
identifiers. Two retained executions of the same plan have different consumer
IDs because their output paths differ; only the exact consumer ID selects one.
There is no plan-ID selection, prefix/suffix matching, abbreviation (including
`39b`), newest-match selection or persistent alias state. A moved or copied
execution does not inherit the original handle. Obtain the current public ID
through status; never edit retained bindings to make a handle match.

The default trusted lookup root is the invoked WLO checkout/installation's
`output/`, derived from the location of its `lib/` and `bin/`, not the current
directory. From elsewhere, invoke that checkout's `bin/wlo` by path. An optional
`--output-root DIR` selects one existing root explicitly; relative overrides
use the current directory. Only immediate subdirectories are candidates;
ordinary root files are ignored. Every candidate must have valid retained
`execution.json` and frozen `plan.json` with matching plan ID and exact byte
hash, original workdir, valid state/import shape, and any frozen profile binding.
Existing owned holds must also bind to that exact execution. Lookup reads these
contracts without preparing an execution, starting a runner, loading workers or
contacting registries. Even an unrelated incomplete/corrupt candidate refuses
discovery, rather than potentially concealing another match. Missing roots or
handles refuse without creating output directories. WLO does not search or fall
back to ancestor, sibling, home, another checkout or production roots.

Lookup requires canonical directory paths: symlink roots, symlink ancestors,
child aliases, linked metadata and linked control destinations are refused,
including broken links. Use the canonical path for `--output-root`. Metadata
and existing pause marker/request files must be regular, singly linked files;
existing control directories must be regular contained directories. Root and
selected-directory device/inode identities, retained identity and controls are
rechecked before handing off to the existing control writer. These checks catch
replacement between lookup and revalidation. They do not provide an atomic
filesystem transaction or prevent hostile replacement after the final check;
the operator must control this tree and avoid concurrent renames/replacement
while issuing a request. Normal atomic runner state updates remain supported.

Choose exactly one of `--execution ID` and `--output DIR`. `--output-root` is
valid only with `--execution`; repeated selector/root/token options are refused,
even if identical. Errors write no control request. The acknowledged output path
is printed on success. For nested or unusual layouts use `pause --output DIR`;
its existing validation and path behavior remain compatible. The stricter
discovery checks above apply to the new selector only.

## Manual and owned controls

`wlo pause --output DIR` retains its manual operator marker. Ordinary
`wlo resume PLAN --workdir WORK --output DIR ...` is explicit manual authority:
it clears outstanding manual and owned pause holds before applying the existing
retry, breaker, execution-identity and worker checks. Resume does not authorize
retry or acknowledge a breaker.

An automatic coordinator must instead request an independent hold:

```
wlo pause --output DIR --owner-token TOKEN
wlo pause --execution ID --owner-token TOKEN
```

Either selector uses the same independent hold semantics. Selection only
discovers the target: it never resumes, releases a hold, retries, acknowledges
a breaker, signals a job or changes attempt evidence. Ordinary manual pause
still writes `requested` plus a newline to `control/pause` and lets active work
finish normally while stopping further dispatch.

TOKEN is a private 64-character lowercase hexadecimal nonce. WLO acknowledges
its SHA-256 fingerprint as `Pause request ID: ID`. The request binds that ID to
the public consumer identity for the exact frozen plan, workdir and output.
Repeating the same valid request is idempotent. Different requests coexist.
Do not use a public fingerprint as a bearer token.

The existing JSON status report adds `pause_control`:

- `contract_version`: `wlo-pause-control/v0.1`;
- `consumer_id`: the same identity as public consumer demand;
- `operator_pause`: whether the manual/legacy marker is present;
- `request_ids`: the outstanding owned-request fingerprints, without tokens.

`paused` is true while any hold remains. Unknown or corrupt owned evidence blocks
execution; it is not inferred to be permission to continue.

Automatic continuation uses foreground resume with one of:

```
wlo resume PLAN --workdir WORK --output DIR ... --release-pause TOKEN
wlo resume PLAN --workdir WORK --output DIR ... --preserve-pauses
```

Scoped release validates the retained request against this execution and deletes
only that hold. An external marker or another owner's hold remains effective,
including a request arriving between public observation and release. Preserve
mode releases nothing. Missing, forged, stale or corrupt ownership is an error;
it never falls back to manual resume. These options are mutually exclusive and
are accepted only by foreground resume, not run, detached start or retry.

The independent request files require no check-and-clear transaction over another
owner's control. Manual pause and owned release act on different filesystem names.
Existing dispatch checks still stop new work when a hold appears; already running
work drains according to the existing graceful pause contract.

An older coordinator phase plus a legacy marker does not establish ownership.
AdventureFinder retains the private token with its transition, verifies the
public binding and exclusive hold, and supplies scoped release. Its explicit
`--continue-operator-pause` path remains the only way it chooses manual resume.
A process restart never inherits a transient manual-continuation flag.

Integrate WLO before the corresponding AdventureFinder update, and verify both
together. Old callers of unscoped resume retain manual semantics; updating WLO
alone does not make an old automatic caller safe. No pause-state migration is
inferred or performed for ambiguous retained coordinator state.
