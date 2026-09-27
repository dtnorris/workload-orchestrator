# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class BootstrapTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)
  CLI = File.join(ROOT, "bin", "wlo")

  def test_version_is_defined
    refute_empty WorkloadOrchestrator::VERSION
  end

  def test_cli_reports_version
    out, err, status = Open3.capture3(RbConfig.ruby, CLI, "--version")

    assert status.success?, err
    assert_equal "#{WorkloadOrchestrator::VERSION}\n", out
  end

  def test_cli_help_lists_runtime_commands
    out, err, status = Open3.capture3(RbConfig.ruby, CLI, "--help")

    assert status.success?, err
    assert_includes out, "bin/wlo validate PLAN.json"
    assert_includes out, "bin/wlo run PLAN.json"
    assert_includes out, "bin/wlo resume PLAN.json"
  end
end
