# frozen_string_literal: true

require_relative "test_helper"

class ExampleTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)

  def test_bundled_example_is_a_valid_plan
    plan = WorkloadOrchestrator::Plan.load(File.join(ROOT, "examples", "hello-plan.json"))

    assert_equal "hello-local", plan.id
    assert_equal 1, plan.jobs.length
    assert_equal 1, plan.pools.length
  end
end
