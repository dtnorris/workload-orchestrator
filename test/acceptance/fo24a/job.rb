# frozen_string_literal: true

require_relative "offline_guard"
require "json"
raise "worker endpoint missing" unless ENV.fetch("WLO_WORKER_ENDPOINT").start_with?("http://127.0.0.1:")
puts JSON.generate("endpoint" => ENV.fetch("WLO_WORKER_ENDPOINT"), "scripted" => true)
Fo24aOfflineGuard.assert_clean!
exit(ARGV.fetch(0) == "failure" ? 7 : 0)
