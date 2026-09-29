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
    class TransportError < Error
      attr_reader :exit_status, :stdout, :stderr

      def initialize(message, exit_status: nil, stdout: "", stderr: "")
        super(message)
        @exit_status = exit_status
        @stdout = stdout
        @stderr = stderr
      end
    end

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
    def dispatch(request:, workdir:, output_dir:, timeout_seconds: nil)
      RpofContract.dispatch_request!(request)
      directory = File.realpath(workdir)
      raise Error, "workdir must be a directory" unless File.directory?(directory)

      output = File.expand_path(output_dir)
      FileUtils.mkdir_p(File.dirname(output))
      Dir.mkdir(output)
      result = invoke_dispatch(
        request, ["--workdir", directory, "--output", output], timeout_seconds: timeout_seconds
      )
      persist_transport(output, result)
      document = read_result(File.join(output, "summary.json"), LEGACY_DISPATCH_SUMMARY)
      RpofContract.dispatch_summary!(document, request, result.exit_status)
      result.document = document.merge("contract_version" => RpofContract::DISPATCH_SUMMARY)
      result.freeze
    rescue Errno::EEXIST
      raise Error, "dispatch output already exists; use a new output directory (provider evidence is preserved)"
    rescue SystemCallError => e
      raise Error, "RPOF dispatch filesystem error: #{e.message}"
    rescue TransportError => e
      persist_transport(output, e) if defined?(output) && output && File.directory?(output)
      raise
    end

    private

    def invoke_dispatch(request, arguments, timeout_seconds:)
      if timeout_seconds && (!timeout_seconds.is_a?(Numeric) || !timeout_seconds.finite? || !timeout_seconds.positive?)
        raise Error, "RPOF dispatch timeout must be positive and finite"
      end
      Tempfile.create(["wlo-rpof-request-", ".json"]) do |file|
        file.write(JSON.generate(request.merge("contract_version" => LEGACY_DISPATCH_REQUEST)))
        file.flush
        owner_reader, owner_writer = IO.pipe
        stdout = stderr = ""
        status = nil
        Open3.popen3(
          [@executable, @executable], "dispatch", "--request", file.path, *arguments,
          "--owner-fd", "3", { 3 => owner_reader, pgroup: true }
        ) do |input, output, error, waiter|
          input.close
          owner_reader.close
          readers = [output, error].map { |io| Thread.new { io.read } }
          completed = timeout_seconds ? waiter.join(timeout_seconds) : waiter.join
          unless completed
            terminate_group(waiter.pid)
            stdout, stderr = readers.map { |reader| reader.value }
            raise TransportError.new(
              "RPOF dispatch timed out at the original paid deadline; outcome is in doubt",
              stdout: stdout, stderr: stderr
            )
          end
          status = waiter.value
          stdout, stderr = readers.map { |reader| reader.value }
        ensure
          readers&.each { |reader| reader.kill if reader.alive? }
          readers&.each(&:join)
        end
        unless status.exited? && [0, 1].include?(status.exitstatus)
          detail = status.signaled? ? "signal #{status.termsig}" : "exit #{status.exitstatus}"
          raise TransportError.new(
            "RPOF dispatch failed (#{detail}): #{stderr.strip}",
            exit_status: status.exitstatus, stdout: stdout, stderr: stderr
          )
        end
        Result.new(exit_status: status.exitstatus, stdout: stdout, stderr: stderr)
      ensure
        owner_writer&.close unless owner_writer&.closed?
        owner_reader&.close unless owner_reader&.closed?
      end
    rescue SystemCallError => e
      raise TransportError, "RPOF dispatch could not run: #{e.message}"
    end

    def terminate_group(pid)
      Process.kill("TERM", -pid)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2.0
      sleep 0.05 while process_group_alive?(pid) && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      Process.kill("KILL", -pid) if process_group_alive?(pid)
    rescue Errno::ESRCH
      nil
    end

    def process_group_alive?(pid)
      Process.kill(0, -pid)
      true
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true
    end

    def persist_transport(output, result)
      File.write(File.join(output, "client-stdout.log"), result.stdout.to_s)
      File.write(File.join(output, "client-stderr.log"), result.stderr.to_s)
      File.write(
        File.join(output, "client.json"),
        JSON.pretty_generate(
          "exit_status" => result.exit_status,
          "transport_error" => (result.is_a?(TransportError) ? result.message : nil)
        ) + "\n"
      )
    end

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
