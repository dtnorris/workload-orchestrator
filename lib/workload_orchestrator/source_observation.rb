# frozen_string_literal: true

require "digest"
require "securerandom"

module WorkloadOrchestrator
  # Retains validated publisher bytes before checkpoint acceptance. These are
  # observations, not proof that a subsequent checkpoint write succeeded.
  class SourceObservation
    # Persist exact stdout/stderr and correlation hashes beside this source's
    # checkpoint. IO/durability errors raise Error and prevent acceptance; no
    # old snapshot is replayed or treated as fresh by this journal.
    def self.retain!(checkpoint_path:, registry:, stderr:, attempted_at:)
      root = File.join(File.dirname(checkpoint_path), "observations")
      raise Error, "source observation path is linked" if Pathname.new(root).ascend.any?(&:symlink?)

      FileUtils.mkdir_p(root, mode: 0o700)
      File.open(File.dirname(root), File::RDONLY, &:fsync)
      temporary = File.join(root, ".pending-#{SecureRandom.hex(16)}")
      FileUtils.mkdir(temporary, mode: 0o700)
      files = { "stdout.bin" => registry.bytes, "stderr.bin" => stderr,
                "receipt.json" => "#{JSON.pretty_generate(receipt(registry, stderr, attempted_at))}\n" }
      files.each { |name, bytes| write_bytes!(File.join(temporary, name), bytes) }
      File.open(temporary, File::RDONLY, &:fsync)
      destination = File.join(root, "#{registry.revision}-#{SecureRandom.hex(16)}")
      File.rename(temporary, destination)
      File.open(root, File::RDONLY, &:fsync)
      destination
    rescue SystemCallError, IOError => e
      raise Error, "cannot retain source observation: #{e.message}"
    end

    # Describe exact source bytes and registry identity without claiming the
    # checkpoint has committed. Returns a serializable, provider-neutral record.
    def self.receipt(registry, stderr, attempted_at)
      {
        "contract_version" => "wlo-source-observation/v0.1",
        "phase" => "validated_before_checkpoint", "attempted_at" => attempted_at.iso8601(6),
        "registry_id" => registry.registry_id, "revision" => registry.revision,
        "snapshot_sha256" => registry.sha256, "worker_count" => registry.entries.length,
        "stdout_sha256" => Digest::SHA256.hexdigest(registry.bytes), "stderr_sha256" => Digest::SHA256.hexdigest(stderr)
      }
    end
    private_class_method :receipt

    # Exclusively create and fsync one binary artifact; IO errors propagate to
    # the caller, which refuses the candidate checkpoint on retention failure.
    def self.write_bytes!(path, bytes)
      File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0o600) do |file|
        file.binmode
        file.write(bytes)
        file.flush
        file.fsync
      end
    end
    private_class_method :write_bytes!
  end
end
