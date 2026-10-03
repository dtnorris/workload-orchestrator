# frozen_string_literal: true

require_relative "test_helper"

class Bd05DomainIndependenceTest < Minitest::Test
  ACTIVE_PUBLIC_FILES = %w[
    lib/workload_orchestrator/runner.rb
    lib/workload_orchestrator/execution_doctor.rb
    lib/workload_orchestrator/execution_watch.rb
    docs/operator-diagnostics.md
  ].freeze
  FORBIDDEN = [
    "AF_OLLAMA_BASE_URL", "AdventureFinder", "inspect_pod", "RunPod",
    "local-ollama-workers", "runpod-ollama-fleet"
  ].freeze

  def test_active_runtime_and_public_contracts_are_domain_neutral
    root = File.expand_path("..", __dir__)

    ACTIVE_PUBLIC_FILES.each do |relative|
      contents = File.read(File.join(root, relative))
      FORBIDDEN.each do |term|
        refute_includes contents, term, "#{relative} contains #{term.inspect}"
      end
    end
  end

  def test_generic_attempt_placement_contract_is_explicit
    assert_equal "wlo-attempt-placement/v0.1", WorkloadOrchestrator::Runner::PLACEMENT_INTERFACE_VERSION
    assert_equal "WLO_WORKER_ENDPOINT", WorkloadOrchestrator::Runner::WORKER_ENDPOINT_ENV
  end
end
