# frozen_string_literal: true

require "json"
require "timeout"

root, id = ARGV
File.write(File.join(root, "#{id}.started"), JSON.generate("pid" => Process.pid,
  "endpoint" => ENV.fetch("WLO_WORKER_ENDPOINT"), "monotonic" => Process.clock_gettime(Process::CLOCK_MONOTONIC)))
Timeout.timeout(20) { sleep 0.01 until File.file?(File.join(root, "#{id}.release")) }
STDOUT.binmode
STDOUT.write("synthetic #{id}\x00\xFF\n".b)
