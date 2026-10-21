# frozen_string_literal: true

module WorkloadOrchestrator
  # Discover one retained execution by its existing public consumer identity.
  # No runner, preparation, directory creation or persistent discovery state.
  class PauseTarget
    # Select an explicit existing output root; relative roots use the caller's
    # current directory. Returns an inert resolver, without filesystem writes.
    def initialize(root)
      raise Error, "output root must not be empty" if root.to_s.empty?

      @root = File.expand_path(root)
    rescue ArgumentError => error
      raise Error, "invalid output root: #{error.message}"
    end

    # Yield exactly one validated output for a full public consumer ID and return
    # the block's result. Invalid, missing or unsafe evidence raises Error before
    # yielding. Recheck directory identity and controls at the write handoff;
    # this is not protection against hostile replacement after the final check.
    def with_output(identifier)
      unless identifier.is_a?(String) && PauseRequests::TOKEN.match?(identifier)
        raise Error, "--execution requires the full 64-hex consumer ID; plan IDs and abbreviations are unsupported"
      end

      root_identity = directory_identity(@root)
      output, output_identity = find_output(identifier)
      unless directory_identity(@root) == root_identity && directory_identity(output) == output_identity &&
             identity_for(output) == identifier
        raise Error, "pause target changed during lookup"
      end

      yield output
    rescue SystemCallError, IOError, JSON::ParserError, KeyError, TypeError, ArgumentError => error
      raise Error, "cannot resolve pause execution: #{error.message}"
    end

    private

    # Inspect only immediate directories, never directory names as authority.
    # Return the unique output/inode pair; incomplete or corrupt children also
    # refuse discovery so they cannot hide another match. No files are changed.
    def find_output(identifier)
      matches = Dir.children(@root).sort.filter_map do |name|
        output = File.join(@root, name)
        stat = File.lstat(output)
        raise Error, "symlink in output root: #{output}" if stat.symlink?
        next unless stat.directory?

        output_identity = directory_identity(output)
        [output, output_identity] if identity_for(output) == identifier
      end
      raise Error, "execution ID not found under #{@root}" if matches.empty?
      raise Error, "ambiguous execution ID under #{@root}" unless matches.one?

      matches.first
    end

    # Return an existing canonical directory's device/inode; reject links in
    # every path component. Root aliases must be supplied as canonical paths.
    def directory_identity(path)
      stat = File.lstat(path)
      unless stat.directory? && File.realpath(path) == path
        raise Error, "pause lookup requires a canonical directory without symlinks: #{path}"
      end

      [stat.dev, stat.ino]
    end

    # Validate frozen plan/state/profile binding through the owning store and
    # pause-request reader. Return the existing consumer ID, including the exact
    # output path; copied/moved outputs therefore cannot inherit another handle.
    def identity_for(output)
      state_path = File.join(output, "execution.json")
      regular_file!(state_path)
      regular_file!(File.join(output, "plan.json"))
      state = JSON.parse(File.binread(state_path))
      raise Error, "execution state must be an object" unless state.is_a?(Hash)
      workdir = state["workdir"]
      unless workdir.is_a?(String) && workdir.start_with?("/") && File.expand_path(workdir) == workdir
        raise Error, "invalid retained workdir"
      end

      plan = Plan.load(File.join(output, "plan.json"))
      profile = File.join(output, "execution-profile.json")
      if state["execution_profile_sha256"]
        regular_file!(profile)
        plan = ExecutionProfile.load(profile).bind(plan)
      elsif File.exist?(profile) || File.symlink?(profile)
        raise Error, "unbound frozen execution profile"
      end
      binding = state["workers_sha256"]
      unless binding.nil? || binding.is_a?(String) && PauseRequests::TOKEN.match?(binding)
        raise Error, "invalid retained worker binding"
      end
      validate_controls!(output)
      ExecutionStore.new(output_dir: output, plan: plan, workdir: workdir,
                         workers_sha256: binding).validate_retained_identity!
      PauseRequests.new(output).snapshot.fetch("consumer_id")
    end

    # Require regular, singly linked metadata/control files; never follow a
    # symlink or write through a hardlinked manual marker. Returns File::Stat.
    def regular_file!(path)
      stat = File.lstat(path)
      raise Error, "unsafe pause lookup file: #{path}" unless stat.file? && stat.nlink == 1

      stat
    end

    # Validate each existing control component before invoking legacy controls.
    # Missing controls are allowed, missing executions are not. Returns nil;
    # unsafe links, special files and malformed holds raise Error without writes.
    def validate_controls!(output)
      control = File.join(output, "control")
      return unless File.exist?(control) || File.symlink?(control)

      directory_identity(control)
      marker = File.join(control, "pause")
      regular_file!(marker) if File.exist?(marker) || File.symlink?(marker)
      requests = File.join(control, "pause-requests")
      return unless File.exist?(requests) || File.symlink?(requests)

      directory_identity(requests)
      Dir.children(requests).each { |name| regular_file!(File.join(requests, name)) }
    end
  end
end
