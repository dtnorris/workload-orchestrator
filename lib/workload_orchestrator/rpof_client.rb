# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "tempfile"
require "tmpdir"
require_relative "rpof_contract"

module WorkloadOrchestrator
  # A process/JSON transport, not a scheduler or paid-resource lifecycle owner.
  class RpofClient
    LEGACY_CAPABILITY_REQUEST = "afio-rpof-capability-check-request/v0.2"
    LEGACY_CAPABILITY_RESULT = "afio-rpof-capability-check-result/v0.1"
    LEGACY_DISPATCH_REQUEST = "afio-rpof-dispatch-request/v0.1"
    LEGACY_DISPATCH_SUMMARY = "afio-rpof-dispatch-summary/v0.1"

    Result = Struct.new(:document, :exit_status, :stdout, :stderr, keyword_init: true)

    def initialize(executable:)
      unless executable.is_a?(String) && executable.start_with?("/") && !executable.include?("\0") &&
             File.file?(executable) && File.executable?(executable)
        raise Error, "RPOF executable must be an absolute path to an executable file"
      end

      @executable = File.realpath(executable)
    end

    def capability_check(request)
      RpofContract.capability_request!(request)
      Dir.mktmpdir("wlo-rpof-capability-") do |directory|
        output = File.join(directory, "result.json")
        result = invoke(request, LEGACY_CAPABILITY_REQUEST, "capability-check", ["--output", output])
        document = read_result(output, LEGACY_CAPABILITY_RESULT)
        RpofContract.capability_result!(document, request, result.exit_status)
        result.document = document.merge("contract_version" => RpofContract::CAPABILITY_RESULT)
        result.freeze
      end
    rescue SystemCallError => e
      raise Error, "RPOF capability filesystem error: #{e.message}"
    end

    # Dispatch to an existing fleet only. A new output directory prevents stale
    # evidence being mistaken for this invocation. WLO-managed resume is step 8.
    def dispatch(request:, workdir:, output_dir:)
      RpofContract.dispatch_request!(request)
      directory = File.realpath(workdir)
      raise Error, "workdir must be a directory" unless File.directory?(directory)

      output = File.expand_path(output_dir)
      FileUtils.mkdir_p(File.dirname(output))
      Dir.mkdir(output)
      result = invoke(request, LEGACY_DISPATCH_REQUEST, "dispatch", ["--workdir", directory, "--output", output])
      document = read_result(File.join(output, "summary.json"), LEGACY_DISPATCH_SUMMARY)
      RpofContract.dispatch_summary!(document, request, result.exit_status)
      result.document = document.merge("contract_version" => RpofContract::DISPATCH_SUMMARY)
      result.freeze
    rescue Errno::EEXIST
      raise Error, "dispatch output already exists; use a new output directory (provider evidence is preserved)"
    rescue SystemCallError => e
      raise Error, "RPOF dispatch filesystem error: #{e.message}"
    end

    private

    def invoke(request, wire_version, operation, arguments)
      Tempfile.create(["wlo-rpof-request-", ".json"]) do |file|
        file.write(JSON.generate(request.merge("contract_version" => wire_version)))
        file.flush
        # Explicit argv, including the executable/argv0 pair: no implicit shell.
        stdout, stderr, status = Open3.capture3(
          [@executable, @executable], operation, "--request", file.path, *arguments
        )
        unless status.exited? && [0, 1].include?(status.exitstatus)
          detail = status.signaled? ? "signal #{status.termsig}" : "exit #{status.exitstatus}"
          raise Error, "RPOF #{operation} failed (#{detail}): #{stderr.strip}"
        end

        Result.new(exit_status: status.exitstatus, stdout: stdout, stderr: stderr)
      end
    rescue SystemCallError => e
      raise Error, "RPOF #{operation} could not run: #{e.message}"
    end

    def read_result(path, expected_version)
      raise Error, "RPOF did not write a non-empty JSON result" unless File.file?(path) && File.size?(path)

      document = JSON.parse(File.read(path))
      raise Error, "RPOF result must be a JSON object" unless document.is_a?(Hash)

      RpofContract.version!(document, expected_version)
      document
    rescue JSON::ParserError => e
      raise Error, "RPOF result is invalid JSON: #{e.message}"
    end
  end
end
