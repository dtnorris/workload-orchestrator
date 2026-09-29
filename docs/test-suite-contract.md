# Test Suite Contract

## Functional suite and runtime baseline

Measured from `0fb73e553e5f0b2310b56fe272ee19e891237ee3` on 2026-09-29
in an agent-local Linux container with Ruby 4.0.6 and the locked gems. One
warm-up run was discarded. Five isolated, ordinary `rake test` runs took
14.587, 14.452, 15.319, 14.576, and 14.261 seconds (min 14.261, median
14.576, mean 14.639, max 15.319). Each ran 135 tests and 963 assertions with
zero failures, errors, and skips. A coverage-instrumented run completed its
Minitest portion in 14.730 seconds; the enclosing `test:coverage` task took
15.163 seconds.

The reported macOS observation before this change was 6.480 seconds for 130
tests on the M4 Pro. It is one observation from an earlier WLO HEAD, so the
macOS thresholds below are provisional until several isolated runs of this
exact 135-test suite are available on that machine. This Linux build runs
materially slower and cannot be used as a Mac timing baseline.

The warning and hard ceiling are 7.5 and 8.0 seconds on macOS, and 17.0 and
18.0 seconds on other platforms. They apply to the coverage-instrumented
functional suite, using a monotonic wall clock. The Linux limits leave modest
headroom over the five measured runs and the 15.163-second timed coverage run.
The macOS limits leave headroom over the reported earlier run while still
exposing a substantial regression. Review `rake test:slow` and recent test
changes before considering a threshold increase.

For deliberate cross-repository contention, `AF_TEST_CONTENDED=1` multiplies
both limits by 1.25. This yields 9.375/10.0 seconds on macOS and 21.25/22.5
seconds on other platforms. Ordinary local runs retain the isolated limits.

## Coverage ratchet

Coverage is opt-in for `rake test` and starts in the Minitest task prelude,
before test files are loaded. `.simplecov` limits coverage to production
`lib/**/*.rb` and enables line and branch tracking. On Ruby 4.0.6, the initial
run covered 2,349 of 2,526 lines (92.99%) and 803 of 1,038 branches
(77.36%) across 28 production files.

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
