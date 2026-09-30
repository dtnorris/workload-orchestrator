# frozen_string_literal: true

require_relative "test_helper"

class Dw33CompatibilityRemovalTest < Minitest::Test
  def test_paid_capacity_flags_are_rejected_before_plan_or_output_access
    %w[--rpof-executable --paid-budget --authorize-paid-rpof].each do |flag|
      out = StringIO.new
      err = StringIO.new
      code = WorkloadOrchestrator::CLI.new(
        ["run", "missing-plan.json", "--workdir", ".", "--output", "missing-output", flag],
        out: out, err: err
      ).run

      assert_equal 1, code
      assert_includes err.string, "invalid option: #{flag}"
      assert_empty out.string
    end
  end
end
