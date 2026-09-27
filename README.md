# workload-orchestrator

`workload-orchestrator` (WLO) is a small Ruby project for deterministic,
resumable execution of declarative command workloads across configured workers.

## Status

Bootstrap only.

This repository currently establishes the project identity, generic directory
layout, command-line entry point, worker-configuration example, and local test
harness. The execution-plan contract and runtime are intentionally not
implemented yet.

## Requirements

- Ruby 4.0.x
- Bundler

## Setup

```bash
bundle install
cp config/workers.example.yml config/workers.yml
```

`config/workers.yml` is machine-local and ignored by Git.

## Commands

```bash
bin/wlo --help
bin/wlo --version
bundle exec rake
script/check
```

## Design boundary

WLO is intended to execute opaque command jobs. Workload-specific systems own
the meaning and generation of those jobs.

See `docs/architecture.md`.
