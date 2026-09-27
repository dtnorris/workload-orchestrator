# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "rbconfig"

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

  def test_cli_help_is_available
    out, err, status = Open3.capture3(RbConfig.ruby, CLI, "--help")

    assert status.success?, err
    assert_includes out, "Usage:"
    assert_includes out, "bin/wlo --version"
  end

  def test_runtime_commands_are_deliberately_absent
    _out, err, status = Open3.capture3(RbConfig.ruby, CLI, "run")

    assert_equal 64, status.exitstatus
    assert_includes err, "runtime commands are not implemented yet"
  end
end
