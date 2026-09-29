# frozen_string_literal: true

# Shared fixture construction only: the tests exercise the real CLI/Runner.
module OperatorFixtures
  def in_process_cli(*args)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(args, out: out, err: err).run
    [out.string, err.string, code]
  end

  def write_operator_profile
    document = JSON.parse(File.read(@plan))
    document["contract_version"] = "wlo-execution-plan/v0.2"
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan, JSON.generate(document))
    path = File.join(@root, "profile.json")
    File.write(path, JSON.generate(
      "contract_version" => "wlo-execution-profile/v0.1",
      "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                    "worker_names" => ["local"], "max_concurrency" => 1 }]
    ))
    path
  end
end
