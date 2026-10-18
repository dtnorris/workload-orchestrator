# frozen_string_literal: true

require_relative "../fo24a/offline_guard"
require "json"
require "timeout"

Fo24aOfflineGuard.component!("workload_orchestrator")
wlo, fixture = ARGV
require File.join(wlo, "lib/workload_orchestrator")
configuration = WorkloadOrchestrator::WorkerSourceConfiguration.load(File.join(fixture, "sources.yml"))
plan_path = File.join(fixture, "input.json")
plan = WorkloadOrchestrator::Plan.load(plan_path)
commands = configuration.entries.map { |entry| entry.source.argv } + plan.jobs.map(&:argv)

# Intentionally remove both consent checks only for the critical-boundary
# negative control. Positive acceptance always uses the unmodified real path.
if File.file?(File.join(fixture, "bypass-exclusion"))
  WorkloadOrchestrator::SourceOffers.prepend(Module.new do
    # Mutation control: deliberately omit scheduler eligibility filtering.
    def eligible_workers(workers, _source_registry_ids)
      workers
    end

    # Mutation control: deliberately omit the final dispatch consent recheck.
    def dispatch(_name)
      yield -> {}
    end
  end)
end

# Permit only exact observational publisher and synthetic payload argv. Nested
# scopes remain active while WLO's owned subprocess groups run and are reaped.
run = lambda do
  Timeout.timeout(25) do
    WorkloadOrchestrator::CLI.new(["resume", plan_path, "--workdir", fixture,
      "--output", File.join(fixture, "execution"), "--worker-sources-config", File.join(fixture, "sources.yml")],
      worker_poll_interval: 0.01).run
  end
end
commands.uniq.reverse_each do |argv|
  continuation = run
  run = -> { Fo24aOfflineGuard.permit(argv, &continuation) }
end
exit run.call
