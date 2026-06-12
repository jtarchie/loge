# frozen_string_literal: true

require_relative "loge/version"

# Loge ships Ruby logs to a loge server's /api/v1/push endpoint. Delivery is
# asynchronous and batched on a background thread, so logging never blocks on
# the network; call #close on shutdown to flush.
#
#   logger = Loge.logger(
#     endpoint: "http://localhost:3000",
#     labels: { "app" => "checkout", "env" => "prod" },
#   )
#   logger.info("user logged in")
#   logger.info(msg: "user logged in", user_id: "abc123")
#   logger.close
#
# Labels are indexed and should stay low-cardinality (app, env, host, level).
# High-cardinality data (trace_id, user_id, request paths) belongs in the log
# line, where loge treats it as a keyword-search target.
module Loge
  class Error < StandardError; end

  # Loge.logger builds a Client and wraps it in a Logger. Logger options are
  # named here; everything else is passed through to Client.new.
  def self.logger(level: ::Logger::INFO, progname: nil, formatter: Formatter.new, level_label: "level",
                  **client_options)
    Logger.new(
      Client.new(**client_options),
      level: level,
      progname: progname,
      formatter: formatter,
      level_label: level_label,
    )
  end
end

require_relative "loge/formatter"
require_relative "loge/client"
require_relative "loge/logger"
require_relative "loge/railtie" if defined?(::Rails::Railtie)
