# frozen_string_literal: true

require "digest"
require "fileutils"
require_relative "terminal_import"

module WorkloadOrchestrator
  module TerminalImportState
    # Import is an explicit, zero-execution handoff. A pending marker blocks all
    # ordinary starts and retries until the same immutable handoff completes.
    def import_terminal!(bytes:)
      handoff = TerminalImport.new(bytes: bytes, plan: plan, workdir: workdir,
                                   workers_sha256: @workers_sha256)
      sources = handoff.rows.transform_values { |row| handoff.source_bytes(row) }
      with_execution_lock do
        prepare!(allow_incomplete_import: true)
        with_lock do
          state = read_execution
          marker = state["terminal_import"]
          if marker
            raise Error, "terminal import differs from existing handoff" unless marker["sha256"] == handoff.sha256
          else
            unless state["status"] == "pending" && !state.key?("started_at") &&
                   !state.key?("retry_history") && !state.key?("dispatch_halt") &&
                   (!File.directory?(File.join(output_dir, "runs")) || Dir.empty?(File.join(output_dir, "runs")))
              raise Error, "terminal import requires a fresh, unstarted execution"
            end
            state["terminal_import"] = { "sha256" => handoff.sha256, "phase" => "applying" }
            write_json(execution_path, state)
          end

          snapshot = File.join(output_dir, "terminal-import.json")
          raise Error, "terminal import snapshot is a symlink" if File.symlink?(snapshot)
          if File.file?(snapshot)
            raise Error, "terminal import snapshot conflicts" unless File.binread(snapshot) == bytes
          else
            atomic_write(snapshot, bytes)
          end
          apply_terminal_import!(handoff, sources)
          state["terminal_import"] = {
            "sha256" => handoff.sha256, "phase" => "complete", "jobs" => handoff.rows.length,
            "completed_at" => timestamp
          }
          state["updated_at"] = timestamp
          write_json(execution_path, state)
          rebuild_jobs_unlocked
        end
      end
      handoff.rows.length
    end

    private

    def apply_terminal_import!(handoff, sources)
      root = File.join(output_dir, "runs")
      raise Error, "terminal import runs directory is a symlink" if File.symlink?(root)
      unexpected = File.directory?(root) ? Dir.children(root) - handoff.rows.keys : []
      raise Error, "terminal import found unrelated job evidence: #{unexpected.join(', ')}" unless unexpected.empty?

      handoff.rows.each do |id, row|
        job = plan.jobs.find { |entry| entry.id == id }
        dir = run_dir(job)
        raise Error, "terminal import job directory is a symlink for #{id}" if File.symlink?(dir)
        FileUtils.mkdir_p(dir)
        extra = Dir.children(dir) - %w[metadata.json import-source]
        raise Error, "terminal import found unrelated attempt evidence for #{id}" unless extra.empty?
        source = File.join(dir, "import-source")
        if File.symlink?(source) || File.symlink?(metadata_path(job))
          raise Error, "terminal import evidence is a symlink for #{id}"
        end
        if File.file?(source)
          raise Error, "terminal import evidence conflicts for #{id}" unless File.binread(source) == sources.fetch(id)
        else
          atomic_write(source, sources.fetch(id))
        end
        expected = handoff.metadata(job, row)
        if File.file?(metadata_path(job))
          raise Error, "terminal import metadata conflicts for #{id}" unless read_json(metadata_path(job), "job metadata") == expected
        else
          write_json(metadata_path(job), expected)
        end
      end
    end

    def validate_terminal_import!(state)
      marker = state.fetch("terminal_import")
      snapshot = File.join(output_dir, "terminal-import.json")
      raise Error, "terminal import snapshot is missing or linked" unless File.file?(snapshot) && !File.symlink?(snapshot)
      bytes = File.binread(snapshot)
      raise Error, "terminal import snapshot changed" unless Digest::SHA256.hexdigest(bytes) == marker["sha256"]
      handoff = TerminalImport.new(bytes: bytes, plan: plan, workdir: workdir,
                                   workers_sha256: @workers_sha256)
      raise Error, "terminal import job count changed" unless marker["jobs"] == handoff.rows.length
      handoff.rows.each do |id, row|
        job = plan.jobs.find { |entry| entry.id == id }
        source = File.join(run_dir(job), "import-source")
        raise Error, "terminal import evidence missing or changed for #{id}" unless !File.symlink?(run_dir(job)) &&
          !File.symlink?(source) && !File.symlink?(metadata_path(job)) && File.file?(source) &&
          Digest::SHA256.file(source).hexdigest == row.fetch("source_sha256")
        metadata = read_json(metadata_path(job), "job metadata")
        # An explicitly authorized retry may advance the attempt; the immutable
        # imported evidence remains in the original attempt archive.
        next if metadata == handoff.metadata(job, row)
        archive = File.join(output_dir, "attempts", id, "attempt-1")
        unless File.file?(File.join(archive, "metadata.json")) &&
               read_json(File.join(archive, "metadata.json"), "imported attempt") == handoff.metadata(job, row)
          raise Error, "terminal import metadata changed without a retained retry archive for #{id}"
        end
      end
    end

  end
end
