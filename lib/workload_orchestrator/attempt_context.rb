# frozen_string_literal: true

require "digest"

module WorkloadOrchestrator
  # Captures provider-neutral dispatch identity before launch. This receipt is
  # provenance only; it grants no execution success or application acceptance.
  module AttemptContext
    ENV_KEYS = %w[WLO_ATTEMPT_CONTEXT WLO_ATTEMPT_CONTEXT_SHA256].freeze

    # Persist one immutable context for a running metadata document. Returns its
    # path/hash reference; conflicts and filesystem failures raise Error/IO errors.
    def persist_attempt_context!(document)
      state = read_execution
      context = {
        "contract_version" => "wlo-attempt-context/v0.1",
        "execution" => { "output_dir" => File.realpath(output_dir),
                         "created_at" => state.fetch("created_at"),
                         "plan_id" => state.fetch("plan_id"),
                         "plan_sha256" => state.fetch("plan_sha256"),
                         "workdir" => state.fetch("workdir") },
        "attempt" => document.slice("job_id", "pool_id", "worker", "attempt", "started_at", "argv",
                                    "worker_execution_identity", "worker_registry_binding", "worker_snapshot")
      }
      bytes = (JSON.pretty_generate(context) + "\n").b
      path = File.join(output_dir, "dispatch-context", document.fetch("job_id"),
                       "attempt-#{document.fetch('attempt')}.json")
      FileUtils.mkdir_p(File.dirname(path))
      if File.exist?(path)
        raise Error, "dispatch context conflicts with retained attempt" unless File.binread(path) == bytes
      else
        File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0o444) { |file| file.write(bytes) }
      end
      { "path" => path, "sha256" => Digest::SHA256.hexdigest(bytes) }
    end

    # Return the reserved environment from the persisted current attempt. Missing
    # or changed context raises Error before a command can be launched.
    def attempt_context_environment(job)
      reference = metadata_for(job).fetch("dispatch_context")
      path = reference.fetch("path")
      unless Digest::SHA256.file(path).hexdigest == reference.fetch("sha256")
        raise Error, "dispatch context changed before launch"
      end
      { ENV_KEYS[0] => path, ENV_KEYS[1] => reference.fetch("sha256") }
    end
  end
end
