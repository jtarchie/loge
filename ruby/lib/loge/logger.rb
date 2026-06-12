# frozen_string_literal: true

require "logger"

module Loge
  # Logger is a ::Logger that renders records through its formatter and hands
  # them to a Loge::Client instead of writing to a log device. It can be used
  # standalone or broadcast to alongside an existing logger (see Railtie).
  class Logger < ::Logger
    attr_reader :client

    def initialize(client, level: ::Logger::INFO, progname: nil, formatter: Loge::Formatter.new,
                   level_label: "level")
      super(nil, level: level, progname: progname)
      @client = client
      @level_label = level_label
      self.formatter = formatter
    end

    def add(severity, message = nil, progname = nil)
      severity ||= UNKNOWN
      return true if severity < level

      progname ||= @progname

      if message.nil?
        if block_given?
          message = yield
        else
          message = progname
          progname = @progname
        end
      end

      time = Time.now
      severity_name = format_severity(severity)
      line = (formatter || Loge::Formatter.new).call(severity_name, time, progname, message)
      labels = @level_label ? {@level_label => severity_name} : {}

      @client.push(line, labels: labels, timestamp_ns: (time.to_r * 1_000_000_000).to_i)

      true
    end
    alias log add

    # ::Logger#<< bypasses severity filtering and the formatter to write
    # straight to the device; keep that contract by pushing the raw message.
    def <<(message)
      @client.push(message.to_s)
    end

    # flush blocks until everything logged so far has been delivered.
    def flush(timeout: nil)
      @client.flush(timeout: timeout)
      self
    end

    def close
      @client.close
    end

    # There is no device to reopen.
    def reopen(_logdev = nil)
      self
    end
  end
end
