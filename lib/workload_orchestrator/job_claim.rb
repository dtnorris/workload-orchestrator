# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "socket"
require "time"

module WorkloadOrchestrator
  class JobClaim
    def initialize(output_dir:)
      @claim_dir = File.join(File.expand_path(output_dir), "claims")
    end

    def synchronize(job_id)
      FileUtils.mkdir_p(@claim_dir)
      File.open(claim_path(job_id), File::RDWR | File::CREAT, 0o644) do |file|
        file.flock(File::LOCK_EX)
        write_state(file, job_id, "claimed")
        yield
      ensure
        write_state(file, job_id, "released") if file
      end
    end

    private

    def claim_path(job_id)
      File.join(@claim_dir, "#{Digest::SHA256.hexdigest(job_id.to_s)}.lock")
    end

    def write_state(file, job_id, state)
      document = {
        "job_id" => job_id.to_s,
        "state" => state,
        "pid" => Process.pid,
        "host" => Socket.gethostname,
        "at" => Time.now.iso8601
      }
      file.rewind
      file.truncate(0)
      file.write("#{JSON.pretty_generate(document)}\n")
      file.flush
    end
  end
end
