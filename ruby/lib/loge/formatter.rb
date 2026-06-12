# frozen_string_literal: true

require "json"

module Loge
  # Formatter renders one log record as a JSON line for the push value. The
  # severity and timestamp are intentionally omitted from the line: loge
  # receives the timestamp out-of-band in the value tuple and the severity as
  # a stream label (mirroring slogloge, which drops slog's "time" attribute).
  #
  # Hash messages merge into the JSON root, exceptions carry their class and
  # backtrace, and anything else becomes {"msg" => message.to_s}.
  class Formatter
    BACKTRACE_LINES = 20

    def call(_severity, _time, progname, message)
      record =
        case message
        when Hash
          message.each_with_object({}) { |(key, value), out| out[key.to_s] = value }
        when Exception
          {
            "msg" => message.message,
            "error" => message.class.name,
            "backtrace" => Array(message.backtrace).first(BACKTRACE_LINES),
          }
        else
          {"msg" => message.to_s}
        end

      record["progname"] = progname if progname && !record.key?("progname")

      JSON.generate(record)
    end
  end
end
