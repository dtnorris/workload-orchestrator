#!/usr/bin/env ruby
# frozen_string_literal: true

# Explicit release acceptance, not part of the single-repository unit suite.
# ruby test/acceptance/fo24b.rb /path/to/local-ollama-workers /path/to/runpod-ollama-fleet
# AF_PROJECT_ROOT may supply the parent of both sibling checkouts instead.
require_relative "fo24a/offline_guard"
require "json"
require "tmpdir"
require "fileutils"
require "timeout"

root = File.expand_path("../..", __dir__)
workspace = ENV["AF_PROJECT_ROOT"]
low = ARGV[0] || (workspace && File.join(workspace, "local-ollama-workers"))
rpof = ARGV[1] || (workspace && File.join(workspace, "runpod-ollama-fleet"))
abort "Pass LOW and RPOF roots, or set AF_PROJECT_ROOT" unless low && rpof && ARGV.length <= 2
roots = { "low" => File.expand_path(low), "rpof" => File.expand_path(rpof) }
roots.each_value { |path| abort "missing component lib: #{path}" unless File.directory?(File.join(path, "lib")) }
Fo24aOfflineGuard.self_check!

run = lambda do |script, args, directory, home|
  argv = [RbConfig.ruby, script, *args]
  env = { "HOME" => home, "PATH" => File.dirname(RbConfig.ruby),
          "RUBYOPT" => "-r#{File.join(__dir__, 'fo24a/offline_guard.rb')}" }
  # Optional executor-provided Ruby dependencies; never repository shims.
  env["RUBYLIB"] = ENV["RUBYLIB"] if ENV["RUBYLIB"]
  stdout, stderr, status = Fo24aOfflineGuard.permit(argv) do
    Timeout.timeout(90) { Open3.capture3(env, *argv, chdir: directory, unsetenv_others: true) }
  end
  raise "#{File.basename(script)} failed:\n#{stdout}\n#{stderr}" unless status.success?
  puts stdout
end

Dir.mktmpdir("fo24b-") do |temporary|
  request = File.join(root, "contracts/ollama-capability-request/v0.1/canonical-valid.json")
  providers = {}
  roots.each do |kind, repo|
    destination = File.join(temporary, "#{kind}.json")
    Dir.mktmpdir("fo24b-producer-") do |isolated|
      FileUtils.cp_r(File.join(repo, "lib"), isolated)
      run.call(File.join(__dir__, "fo24a/provider.rb"), [kind, isolated, request, destination], isolated, temporary)
    end
    providers[kind] = JSON.parse(File.binread(destination)).fetch("snapshots")
  end
  input = File.join(temporary, "public-input.json")
  File.write(input, JSON.generate("request" => JSON.parse(File.binread(request)), "providers" => providers))
  Dir.mktmpdir("fo24b-consumer-") do |isolated|
    FileUtils.cp_r(File.join(root, "lib"), isolated)
    raise "unexpected sibling repository" unless Dir.children(isolated) == ["lib"]
    run.call(File.join(__dir__, "fo24b/consumer.rb"), [isolated, input], isolated, temporary)
  end

  # Reuse the owner's real authority tests and tiny tracked fixtures. No .env,
  # credentials, generated provider state, AdventureFinder or AFW is copied.
  Dir.mktmpdir("fo24b-authority-") do |isolated|
    %w[lib bin test config contracts script].each do |name|
      source = File.join(roots.fetch("rpof"), name)
      FileUtils.cp_r(source, isolated) if File.directory?(source)
    end
    run.call(File.join(__dir__, "fo24b/authority.rb"), [isolated, root, input], isolated, temporary)
  end
end
Fo24aOfflineGuard.assert_clean!
puts "FO-24B PASS; all provider actions scripted, no network or inference"
