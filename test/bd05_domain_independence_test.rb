# frozen_string_literal: true

require_relative "test_helper"
require "open3"

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

  def test_loaded_features_contain_no_sibling_implementation
    # The shared test process may load publishers for cross-repository tests.
    # Inspect a fresh WLO runtime so those test-only imports cannot contaminate it.
    root = File.expand_path("..", __dir__)
    script = <<~'RUBY'
      require "json"
      require "workload_orchestrator"
      require File.join(ARGV.fetch(0), "test/support/sibling_implementation_boundary")
      forbidden = SiblingImplementationBoundary.forbidden_features(
        $LOADED_FEATURES, root: ARGV.fetch(0),
        pattern: /(?:adventure[_-]finder|af[_-]workloads|local[_-]ollama[_-]workers|runpod[_-]ollama[_-]fleet)/i
      )
      puts JSON.generate(forbidden)
    RUBY
    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby, "-I#{File.join(root, 'lib')}", "-e", script, root, chdir: root
    )

    assert status.success?, stderr
    assert_empty JSON.parse(stdout)
  end

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
