# Test Suite Contract

## Functional suite and runtime baseline

The current macOS runtime baseline was measured on 2026-09-29 from commit
`9774795c4fdbe0304336d79f470903dae38b65e9` on the primary local development
machine:

- MacBook Pro M4 Pro, 48 GB
- Ruby 4.0.6
- 152 tests
- 1,045 assertions
- 0 failures
- 0 errors
- 0 skips

One warm-up run was discarded before each measurement series.

Five isolated `rake test:coverage` runs measured the complete
coverage-instrumented functional suite protected by the runtime guard:

- 9.173 s
- 9.934 s
- 9.502 s
- 9.696 s
- 9.353 s

Summary:

- min: 9.173 s
- median: 9.502 s
- mean: 9.532 s
- max: 9.934 s
- stddev: approximately 0.265 s

Five isolated ordinary `rake test` wall-clock runs were also measured:

- 9.16 s
- 9.07 s
- 9.43 s
- 9.51 s
- 9.44 s

Summary:

- min: 9.07 s
- median: 9.43 s
- mean: 9.32 s
- max: 9.51 s

Coverage instrumentation therefore adds little material runtime on this suite.
The coverage-instrumented measurement remains authoritative for the runtime
guard because `test:coverage` is the canonical functional execution used by
the full test contract.

The warning and hard ceiling are:

- macOS warning: 11.0 s
- macOS hard failure: 12.0 s
- other-platform warning: 17.0 s
- other-platform hard failure: 18.0 s

The macOS limits intentionally leave measurable headroom above the observed
9.502-second median and 9.934-second maximum without normalizing a future
multi-second regression. Runtime thresholds should not be loosened merely
because the suite grows slower; inspect `rake test:slow` and recent test
changes first.

The non-macOS limits remain separate because the prior Linux execution
environment ran the suite materially slower than the M4 Pro and is not an
appropriate source for the macOS baseline.

For deliberate cross-repository contention, `AF_TEST_CONTENDED=1` multiplies
both limits by 1.25. This yields:

- macOS warning: 13.75 s
- macOS hard failure: 15.0 s
- other-platform warning: 21.25 s
- other-platform hard failure: 22.5 s

Ordinary local runs retain the isolated limits. The contention allowance is
for intentional concurrent repository testing; it does not redefine the
isolated performance baseline.

## Coverage ratchet

Coverage is opt-in for `rake test` and starts in the Minitest task prelude,
before test files are loaded. `.simplecov` limits coverage to production
`lib/**/*.rb` and enables line and branch tracking.

At the post-optimization M4 baseline above, the coverage-instrumented suite
covered 2,508 of 2,695 lines (93.06%) and 867 of 1,127 branches (76.92%).

`.simplecov_baseline.yml` records per-file line and branch floors. A drop below
a file's committed floor fails coverage; an ordinary run never rewrites the
baseline. `rake test:coverage:baseline` is the explicit initialization command.
After deliberate coverage improvements, `bundle exec simplecov ratchet`
tightens existing floors without lowering them.

## Tasks

- `rake test` runs the uninstrumented functional suite.
- `rake test:deps` runs each test file in isolation to expose load-order
  dependencies.
- `rake test:slow` shows the slowest tests.
- `rake test:lint` checks structural Minitest correctness using the focused
  `.rubocop-test.yml` configuration. The existing general `.rubocop.yml`
  remains available for broader Ruby style checks.
- `rake test:coverage` runs the functional suite once with line and branch
  coverage, the committed ratchet, and the runtime guard.
- Plain `rake` runs `test:coverage` first, then runs `test:deps` and
  `test:lint` in parallel. Successful secondary checks are quiet until the
  final summary. Failure diagnostics and any deferred runtime warning or
  failure appear at the end.

The ordinary suite is expected to have zero skips. Scale-sensitive benchmarks
in other AdventureFinder repositories guard their own algorithms; WLO does
not add a synthetic benchmark as part of this general test-health contract.
