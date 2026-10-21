# frozen_string_literal: true

require "digest"

module WorkloadOrchestrator
  # Strict, operator-digest-bound evidence for acknowledging one stopped loss.
  # This value validates retained bytes; it grants no execution or retry itself.
  class WorkerLossReview
    KEYS = %w[
      contract_version action operator reason plan_id plan_sha256 workdir
      execution_sha256 jobs_sha256 workdir_evidence_sha256 job_id attempt attempts preserved_files preserved_directories
    ].freeze

    # Preserve duplicate-key rejection through nested JSON objects.
    class ReviewObject < Hash
      # JSON insertion hook; a repeated field raises JSON::ParserError.
      def []=(key, value)
        raise JSON::ParserError, "duplicate review field #{key.inspect}" if key?(key)

        super
      end
    end

    attr_reader :document, :bytes, :sha256

    # Parse at most 1 MiB of exactly pinned JSON; bad digests/shapes raise Error.
    def initialize(bytes:, sha256:)
      raise Error, "worker-loss review exceeds 1 MiB" if bytes.bytesize > 1_048_576
      raise Error, "worker-loss review SHA-256 differs" unless Digest::SHA256.hexdigest(bytes) == sha256

      @bytes = bytes
      @sha256 = sha256
      @document = JSON.parse(bytes, object_class: ReviewObject)
      validate_shape!
    end

    # Recheck identity, each preserved file/directory and reviewed attempt pins.
    # No files are written; changed, linked or unbound evidence raises Error.
    def validate!(plan:, workdir:, output_dir:)
      unless document.values_at("plan_id", "plan_sha256", "workdir") == [plan.id, plan.sha256, workdir]
        raise Error, "worker-loss review execution identity differs"
      end

      document.fetch("preserved_files").each { |path, hash| validate_file!(workdir, path, hash) }
      document.fetch("preserved_directories").each { |path, entries| validate_directory!(workdir, path, entries) }
      validate_attempt_pins!(plan, workdir, output_dir)
    end

    private

    # Enforce the finite review schema and mandatory nonblank human attribution.
    def validate_shape!
      unless document.is_a?(Hash) && document.keys.sort == KEYS.sort &&
             document["contract_version"] == "wlo-worker-loss-review/v0.1" &&
             document["action"] == "acknowledge_loss_without_retry"
        raise Error, "invalid worker-loss review fields"
      end

      validate_review_values!
    end

    # Validate attribution and finite collection types after the schema keys.
    # Invalid human fields, missing evidence or an invalid attempt raise Error.
    def validate_review_values!
      %w[operator reason].each do |key|
        unless document[key].is_a?(String) && !document[key].strip.empty?
          raise Error, "worker-loss review requires #{key}"
        end
      end
      %w[attempts preserved_files preserved_directories].each do |key|
        raise Error, "worker-loss review requires #{key}" unless document[key].is_a?(Hash)
      end
      unless !document["preserved_files"].empty? && document["attempt"].is_a?(Integer) && document["attempt"].positive?
        raise Error, "worker-loss review requires evidence and a positive attempt"
      end
    end

    # Resolve a relative path without traversal or any symlink ancestor.
    # Invalid paths raise Error before reading their evidence.
    def evidence_path!(workdir, relative)
      unless relative.is_a?(String) && !relative.empty? && !Pathname.new(relative).absolute? &&
             !relative.split("/").intersect?(%w[. ..])
        raise Error, "invalid worker-loss evidence path"
      end

      path = File.join(workdir, relative)
      raise Error, "reviewed worker-loss evidence is linked: #{relative}" if Pathname.new(path).ascend.any?(&:symlink?)

      path
    end

    # Match an exact regular file and digest; invalid hashes/changed bytes raise Error.
    def validate_file!(workdir, relative, digest)
      path = evidence_path!(workdir, relative)
      unless digest.is_a?(String) && digest.match?(/\A[0-9a-f]{64}\z/) &&
             File.file?(path) && Digest::SHA256.file(path).hexdigest == digest
        raise Error, "reviewed worker-loss evidence changed: #{relative}"
      end
    end

    # Preserve membership of original evidence directories while allowing new
    # pending-job directories outside those explicitly reviewed roots.
    def validate_directory!(workdir, relative, entries)
      path = evidence_path!(workdir, relative)
      return if File.directory?(path) && Dir.children(path).sort == entries

      raise Error, "reviewed worker-loss evidence directory changed: #{relative}"
    end

    # Require every named original attempt to be independently pinned among the
    # preserved files. Unknown jobs or omitted metadata evidence raise Error.
    def validate_attempt_pins!(plan, workdir, output_dir)
      document.fetch("attempts").each do |id, digest|
        raise Error, "worker-loss review names unknown attempt" unless plan.jobs.any? { |job| job.id == id }

        path = File.join(output_dir, "runs", id, "metadata.json")
        relative = Pathname.new(path).relative_path_from(Pathname.new(workdir)).to_s
        unless document.fetch("preserved_files")[relative] == digest
          raise Error, "worker-loss review omits attempt evidence"
        end
      end
    end
  end
end
