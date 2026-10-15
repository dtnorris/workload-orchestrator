# Manual and scoped pause continuation

`wlo pause --output DIR` retains its manual operator marker. Ordinary
`wlo resume PLAN --workdir WORK --output DIR ...` is explicit manual authority:
it clears outstanding manual and owned pause holds before applying the existing
retry, breaker, execution-identity and worker checks. Resume does not authorize
retry or acknowledge a breaker.

An automatic coordinator must instead request an independent hold:

```
wlo pause --output DIR --owner-token TOKEN
```

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
