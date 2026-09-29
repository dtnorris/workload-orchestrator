# frozen_string_literal: true

require "digest"
require "json"
require "pathname"

module WorkloadOrchestrator
  # A frozen, evidence-backed description of terminal work predating WLO.
  class TerminalImport
    CONTRACT_VERSION = "wlo-terminal-import/v0.1"
    DIGEST = /\A[0-9a-f]{64}\z/
    CLASSES = %w[operational non_operational].freeze
    MAX_SOURCE_BYTES = 1_048_576

    attr_reader :bytes, :sha256, :rows

    def initialize(bytes:, plan:, workdir:, workers_sha256:)
      @bytes = bytes
      @sha256 = Digest::SHA256.hexdigest(bytes)
      @workdir = File.expand_path(workdir)
      @source_root = File.realpath(@workdir)
      data = JSON.parse(bytes)
      unless data.is_a?(Hash) && data.keys.sort == %w[contract_version execution_profile_sha256 jobs plan_id plan_sha256 workdir workers_sha256]
        raise Error, "terminal import has missing or unknown fields"
      end
      unless data["contract_version"] == CONTRACT_VERSION && data["plan_id"] == plan.id &&
             data["plan_sha256"] == plan.sha256 && data["workdir"] == @workdir &&
             data["execution_profile_sha256"] == plan.execution_profile&.sha256 &&
             data["workers_sha256"] == workers_sha256
        raise Error, "terminal import execution identity differs from the bound plan, profile, workers or workdir"
      end
      jobs = data.fetch("jobs")
      raise Error, "terminal import jobs must be a non-empty array" unless jobs.is_a?(Array) && !jobs.empty?

      known = plan.jobs.to_h { |job| [job.id, job] }
      @rows = jobs.to_h do |row|
        unless row.is_a?(Hash) && row.keys.sort == %w[exit_status failure_class job_id source_path source_sha256 status]
          raise Error, "terminal import job has missing or unknown fields"
        end
        job = known[row["job_id"]]
        raise Error, "terminal import references an unknown job" unless job
        status = row["status"]
        exit_status = row["exit_status"]
        classification = row["failure_class"]
        unless (status == "complete" && exit_status == 0 && classification.nil?) ||
               (status == "failed" && (exit_status.nil? || exit_status.is_a?(Integer) && (1..255).cover?(exit_status)) &&
                CLASSES.include?(classification))
          raise Error, "terminal import has invalid terminal outcome for #{job.id}"
        end
        digest = row["source_sha256"]
        raise Error, "terminal import has invalid evidence digest for #{job.id}" unless digest.is_a?(String) && digest.match?(DIGEST)
        source = row["source_path"]
        unless source.is_a?(String) && !source.empty? && !source.include?("\0") && !Pathname.new(source).absolute? &&
               source.split(File::SEPARATOR).none? { |part| part == ".." }
          raise Error, "terminal import has invalid evidence path for #{job.id}"
        end
        [job.id, row]
      end
      raise Error, "terminal import contains duplicate job IDs" unless @rows.length == jobs.length
    rescue JSON::ParserError, Errno::ENOENT => e
      raise Error, "invalid terminal import: #{e.message}"
    end

    def source_bytes(row)
      path = File.realpath(File.join(@workdir, row.fetch("source_path")))
      unless path.start_with?("#{@source_root}#{File::SEPARATOR}") && File.file?(path) && File.size(path) <= MAX_SOURCE_BYTES
        raise Error, "terminal import evidence is outside the workdir, missing or too large: #{row.fetch('source_path')}"
      end
      content = File.binread(path)
      unless Digest::SHA256.hexdigest(content) == row.fetch("source_sha256")
        raise Error, "terminal import evidence digest changed: #{row.fetch('source_path')}"
      end
      content
    rescue SystemCallError => e
      raise Error, "terminal import evidence unavailable: #{e.message}"
    end

    def metadata(job, row)
      {
        "job_id" => job.id, "pool_id" => job.pool_id, "worker" => "imported",
        "status" => row.fetch("status"), "attempt" => 1, "exit_status" => row.fetch("exit_status"),
        "failure_class" => row.fetch("failure_class"),
        "import_source" => row.slice("source_path", "source_sha256")
      }
    end
  end
end
