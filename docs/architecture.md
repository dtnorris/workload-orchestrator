# Architecture

## Purpose

WLO is a generic command-workload execution layer.

Its eventual responsibility is to accept already-compiled execution intent and
run it safely across configured workers. It does not own the domain semantics
that produced the workload.

## Ownership boundary

Upstream workload systems own:

- domain-specific selection and policy;
- compilation of domain intent into executable jobs;
- domain validation and result interpretation;
- domain provenance.

WLO owns execution concerns such as:

- plan validation;
- worker selection;
- command dispatch;
- concurrency;
- pause and resume;
- failure handling;
- execution evidence.

Provider-specific resource lifecycle may remain in separate tools and is not
part of this bootstrap.

## M1 bootstrap scope

Implemented:

- project/module identity;
- CLI help and version;
- generic local worker-configuration example;
- Minitest/Rake test harness;
- local static check script.

Explicitly deferred:

- execution-plan schema;
- job scheduling;
- worker readiness checks;
- command execution;
- job claims;
- persisted run state;
- pause/resume behavior;
- circuit breaking;
- paid-resource integration.
