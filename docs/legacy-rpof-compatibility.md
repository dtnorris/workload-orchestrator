# Historical RPOF readability after DW-33

WLO executes dynamic v0.3 jobs locally against registry-selected endpoints.
RPOF independently owns capacity, budget, deadline, guardian and teardown.

DW-33 removed `LegacyRpofRunner`, the three provider clients, paid lifecycle,
execution-pool translation/fulfillment, readiness adapter and worker admission.
The CLI no longer accepts `--rpof-executable`, `--paid-budget` or
`--authorize-paid-rpof`. Run, start, resume and worker-check reject RPOF profiles
before accessing workers, creating output or running commands.

`ExecutionProfile`, `PaidBudget`, historical contract validators and execution
state/report/retry/import readers remain for inspection and audit. Historical
v0.1/v0.2 plans and provider evidence are not migrated or rewritten. Local and
fixed-remote profile execution remains supported for v0.2 compatibility. v0.3
rejects every execution profile so a compatibility overlay cannot silently
replace its dynamic registry authority.

The earlier budget, fulfillment, client, readiness and remote-execution documents
are historical references; their old executable instructions are retired.
