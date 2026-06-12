# frozen_string_literal: true

require "json"
require "net/http"
require "uri"

module Loge
  # Client batches rendered log lines and POSTs them to a loge server's
  # /api/v1/push endpoint from a single background thread, mirroring the Go
  # slogloge sender: logging never blocks on the network, a full queue drops
  # records by default, and close performs a final draining flush.
  #
  # Stream labels are indexed by the server and should stay low-cardinality
  # (app, env, host, level); the server rejects streams with no labels at all.
  # High-cardinality data belongs in the line itself.
  class Client
    # FlushRequest asks the worker to deliver everything accepted so far and
    # then signal the waiting caller.
    FlushRequest = Struct.new(:done)
    private_constant :FlushRequest

    # Records dropped because the queue was full (or the client was closed).
    attr_reader :dropped

    def initialize(
      endpoint: "http://localhost:3000",
      path: "/api/v1/push",
      labels: {},
      batch_size: 100,
      flush_interval: 1.0,
      queue_capacity: 4096,
      bearer_token: nil,
      max_retries: 2,
      open_timeout: 5,
      read_timeout: 30,
      block_on_full: false,
      on_error: ->(error) { warn("loge: #{error.message}") }
    )
      @uri = URI.parse(endpoint.to_s.chomp("/") + path)
      raise ArgumentError, "endpoint must be an http(s) URL, got #{endpoint.inspect}" unless @uri.is_a?(URI::HTTP)

      @static_labels = stringify_labels(labels).freeze
      @batch_size = Integer(batch_size)
      @flush_interval = Float(flush_interval)
      @queue_capacity = Integer(queue_capacity)
      @bearer_token = bearer_token
      @max_retries = Integer(max_retries)
      @open_timeout = open_timeout
      @read_timeout = read_timeout
      @block_on_full = block_on_full
      @on_error = on_error

      @lock = Mutex.new
      @dropped = 0
      @closed = false

      @lock.synchronize { spawn_worker }
    end

    # push enqueues one rendered log line; labels are merged over the client's
    # static labels. It never raises and, unless block_on_full, never blocks:
    # a full queue drops the record and bumps the dropped counter.
    def push(line, labels: {}, timestamp_ns: Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond))
      queue = ensure_worker
      return record_drop if queue.nil?

      item = [@static_labels.merge(stringify_labels(labels)), timestamp_ns.to_s, line.to_s.scrub]
      queue.push(item, !@block_on_full)
      nil
    rescue ThreadError, ClosedQueueError
      record_drop
    end

    # flush blocks until every record accepted before the call has been
    # delivered (or routed to on_error).
    def flush(timeout: nil)
      queue = ensure_worker
      return self if queue.nil?

      done = Queue.new
      queue.push(FlushRequest.new(done))
      done.pop(timeout: timeout)
      self
    rescue ClosedQueueError
      self
    end

    # close stops the worker after a final draining flush. It is idempotent
    # and reports any lifetime drops through on_error.
    def close(timeout: nil)
      worker = nil

      @lock.synchronize do
        return if @closed

        @closed = true
        @queue.close
        worker = @worker
      end

      worker.join(timeout)
      report_drops
      nil
    end

    private

    # ensure_worker returns the live queue, restarting the worker after a fork
    # or an unexpected worker death. Returns nil once closed.
    def ensure_worker
      return nil if @closed
      return @queue if @pid == Process.pid && @worker.alive?

      @lock.synchronize do
        return nil if @closed

        spawn_worker unless @pid == Process.pid && @worker.alive?
        @queue
      end
    end

    def spawn_worker
      # After a fork the parent's worker thread does not exist in the child,
      # and anything already queued is the parent's to deliver — start over
      # with an empty queue so those records are not sent twice.
      @pid = Process.pid
      @queue = SizedQueue.new(@queue_capacity)
      @worker = Thread.new(@queue) { |queue| run(queue) }
      @worker.name = "loge-client"
    end

    # run owns the batch and is the only thread that POSTs. It delivers when
    # the batch reaches batch_size, on the flush interval, on an explicit
    # flush, and once more while draining on close.
    def run(queue)
      batch = []
      deadline = monotonic + @flush_interval

      loop do
        item = queue.pop(timeout: [deadline - monotonic, 0].max)

        case item
        when nil
          break if queue.closed?

          deadline = monotonic + @flush_interval
          send_batch(batch)
        when FlushRequest
          signals = drain(queue, batch)
          signals << item.done
          deadline = monotonic + @flush_interval
          send_batch(batch)
          signals.each { |done| done.push(true) }
        else
          batch << item

          if batch.size >= @batch_size
            deadline = monotonic + @flush_interval
            send_batch(batch)
          end
        end
      end

      signals = drain(queue, batch)
      send_batch(batch)
      signals.each { |done| done.push(true) }
    ensure
      close_connection
    end

    # drain moves everything currently queued into batch without blocking and
    # returns the done signals of any pending flush requests.
    def drain(queue, batch)
      signals = []

      loop do
        item = queue.pop(timeout: 0)
        break if item.nil?

        item.is_a?(FlushRequest) ? signals << item.done : batch << item
      end

      signals
    end

    # send_batch groups items sharing a label set into Loki-style streams and
    # delivers them. The batch is consumed either way.
    def send_batch(batch)
      return if batch.empty?

      body = begin
        JSON.generate(payload(batch))
      rescue StandardError => error
        batch.clear
        @on_error.call(error)
        return
      end

      batch.clear
      deliver(body)
    end

    def payload(batch)
      streams = batch.group_by { |labels, _, _| labels }.map do |labels, items|
        {
          "stream" => labels,
          "values" => items.map { |_, nanos, line| [nanos, line] },
        }
      end

      {"streams" => streams}
    end

    # deliver POSTs the body, retrying up to max_retries more times. Any 2xx
    # is success; the final failure goes to on_error.
    def deliver(body)
      attempts = 0

      begin
        response = post(body)
        code = response.code.to_i
        raise Error, "loge push returned #{code}" unless (200..299).cover?(code)
      rescue StandardError => error
        close_connection
        attempts += 1
        retry if attempts <= @max_retries

        @on_error.call(error)
      end
    end

    def post(body)
      request = Net::HTTP::Post.new(@uri.request_uri)
      request["Content-Type"] = "application/json"
      request["Authorization"] = "Bearer #{@bearer_token}" unless @bearer_token.to_s.empty?
      request.body = body
      connection.request(request)
    end

    # connection keeps one Net::HTTP session alive across batches; delivery
    # errors reset it so the next attempt reconnects.
    def connection
      @http ||= begin
        http = Net::HTTP.new(@uri.host, @uri.port)
        http.use_ssl = @uri.scheme == "https"
        http.open_timeout = @open_timeout
        http.read_timeout = @read_timeout
        http.write_timeout = @read_timeout
        http.start
      end
    end

    def close_connection
      @http&.finish
    rescue IOError
      # already closed
    ensure
      @http = nil
    end

    def record_drop
      @lock.synchronize { @dropped += 1 }
      nil
    end

    def report_drops
      return unless @dropped.positive?

      @on_error.call(Error.new("dropped #{@dropped} records because the queue was full"))
    end

    def stringify_labels(labels)
      labels.each_with_object({}) { |(key, value), out| out[key.to_s] = value.to_s }
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
