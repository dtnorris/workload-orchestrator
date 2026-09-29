# frozen_string_literal: true

require_relative "paid_budget_test"
require "minitest/mock"

class RpofBudgetClientTest < Minitest::Test
  include PaidBudgetFixtures

  class Transport < WorkloadOrchestrator::RpofBudgetClient
    attr_reader :calls, :request
    attr_accessor :response, :code

    def initialize(executable:)
      super
      @calls = []
      @code = 0
    end

    private

    def capture_budget(arguments, timeout_seconds: nil)
      @calls << arguments
      @request = JSON.parse(File.read(arguments[2])) if arguments[0] == "arm"
      status = Struct.new(:success?).new(@code.zero?)
      [@response.is_a?(String) ? @response : JSON.generate(@response), "fixture failure", status]
    end
  end

  def setup
    @root = Dir.mktmpdir("wlo budget ; test-")
    @executable = File.join(@root, "rpof ; fixture")
    File.write(@executable, "#!#{RbConfig.ruby}\n")
    File.chmod(0o700, @executable)
    @client = Transport.new(executable: @executable)
    @client.response = snapshot
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_arm_translates_budget_without_extra_provider_fields
    @client.arm_budget(budget: make_budget)
    assert_equal make_budget.provider_request, @client.request
    refute File.exist?(@client.calls.first[2])
  end

  def test_evaluate_heartbeat_guardian_and_teardown_use_exact_identity
    budget = make_budget
    args = ["--budget-id", budget.identity["budget_id"], "--plan-sha256", budget.identity["plan_sha256"]]
    @client.budget_status(budget: budget)
    @client.heartbeat_budget(budget: budget)
    @client.response = guardian
    @client.guardian_status(budget: budget)
    @client.response = snapshot.merge("state" => "TEARDOWN_REQUIRED")
    @client.begin_budget_teardown(budget: budget, reason: "finished ; literal")
    assert_equal [["evaluate", *args], ["heartbeat", *args, "--source", "orchestrator"],
                  ["guardian-status", *args], ["begin-teardown", *args, "--reason", "finished ; literal"]], @client.calls
    %i[close_budget disable_guardian fulfill_execution_pool scale_fleet].each do |method|
      refute_respond_to @client, method
    end
  end

  def test_rejects_bad_json_exit_identity_limits_and_teardown_acknowledgement
    ["{", "null", "[]", snapshot.merge("budget_id" => "other"), snapshot.merge("limits" => {})].each do |response|
      @client.response = response
      assert_raises(WorkloadOrchestrator::Error) { @client.budget_status(budget: make_budget) }
    end
    @client.response = snapshot
    @client.code = 1
    assert_raises(WorkloadOrchestrator::Error) { @client.arm_budget(budget: make_budget) }
    @client.code = 0
    assert_raises(WorkloadOrchestrator::Error) do
      @client.begin_budget_teardown(budget: make_budget, reason: "finished")
    end
  end

  def test_real_process_receives_literal_arguments_without_a_shell
    File.write(@executable, <<~RUBY)
      #!#{RbConfig.ruby}
      require "json"
      puts JSON.generate("argv" => ARGV)
    RUBY
    client = WorkloadOrchestrator::RpofBudgetClient.new(executable: @executable)
    budget = make_budget(declaration.merge("budget_id" => "$(touch injected); literal"))
    response = client.guardian_status(budget: budget)
    assert_equal ["budget", "guardian-status", "--budget-id", budget.identity["budget_id"],
                  "--plan-sha256", budget.identity["plan_sha256"]], response["argv"]
    refute File.exist?(File.join(@root, "injected"))
  end

  def test_hung_process_times_out_and_is_reaped
    marker = File.join(@root, "pid")
    File.write(@executable, <<~RUBY)
      #!#{RbConfig.ruby}
      File.write(#{marker.inspect}, Process.pid.to_s)
      sleep 60
    RUBY
    client = WorkloadOrchestrator::RpofBudgetClient.new(executable: @executable, timeout_seconds: 0.05)
    real_popen3 = Open3.method(:popen3)
    # Start the command's deadline only after the fixture has entered its hang.
    # Otherwise a busy host can spend the entire deadline starting Ruby, and
    # the test never observes a process whose termination it can verify.
    Open3.stub(:popen3, lambda { |*args, **options, &block|
      real_popen3.call(*args, **options) do |input, output, error, waiter|
        startup_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
        until File.file?(marker)
          flunk "fixture exited before writing its PID" if waiter.join(0)
          if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= startup_deadline
            begin
              Process.kill("KILL", -waiter.pid)
            rescue Errno::ESRCH
              nil
            end
            flunk "fixture did not start within 5 seconds"
          end
          sleep 0.005
        end
        block.call(input, output, error, waiter)
      end
    }) do
      error = assert_raises(WorkloadOrchestrator::Error) { client.guardian_status(budget: make_budget) }
      assert_includes error.message, "timed out"
    end
    pid = Integer(File.read(marker))
    assert_raises(Errno::ESRCH) { Process.kill(0, pid) }
  end
end
