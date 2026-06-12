# loge (Ruby)

Ship Ruby and Rails logs to a [loge](https://github.com/jtarchie/loge) server.

`Loge.logger` returns a `::Logger` whose records are rendered as JSON lines and
pushed to the server's `POST /api/v1/push` endpoint. Delivery is asynchronous
and batched on a background thread, so logging never blocks on the network;
call `close` on shutdown to flush. The gem depends only on the standard
library.

## Installation

```ruby
gem "loge"
```

## Usage

```ruby
require "loge"

logger = Loge.logger(
  endpoint: "http://localhost:3000",
  labels: { "app" => "checkout", "env" => "prod" },
)

logger.info("user logged in")                       # {"msg":"user logged in"}
logger.info(msg: "user logged in", user_id: "abc")  # hashes merge into the line
logger.error(exception)                             # class + message + backtrace

logger.close # drains the queue on shutdown
```

Each record becomes one push value: the timestamp is sent out-of-band in
nanoseconds and the severity is attached as a `level` stream label, so the line
itself stays clean JSON.

Labels are indexed by the server and should stay low-cardinality (`app`, `env`,
`host`, `level`). High-cardinality data (trace ids, user ids, request paths)
belongs in the log line, where loge treats it as a keyword-search target. The
server rejects records with no labels at all, so configure at least one static
label when using `Loge::Client` directly.

## Rails

The gem ships a Railtie that is a no-op until configured. Set an endpoint and
every `Rails.logger` record is also broadcast to loge — existing stdout/file
logging stays untouched:

```ruby
# config/environments/production.rb
config.loge.endpoint = "http://loge.internal:3000"   # or set LOGE_ENDPOINT
config.loge.labels = { "region" => "us-east-1" }     # merged over app + env defaults
config.loge.level = :info                            # defaults to Rails.logger.level
```

The stream labels default to `app` (derived from the application class) and
`env` (`Rails.env`). Set `config.loge.enabled = false` to turn the integration
off even when `LOGE_ENDPOINT` is present. Any `Loge::Client` option
(`batch_size`, `bearer_token`, `on_error`, ...) can be set the same way.

## Configuration

Options for `Loge.logger` / `Loge::Client.new`, mirroring the Go
[`slogloge`](../slogloge/) handler:

| Option           | Default                   | Meaning                                              |
| ---------------- | ------------------------- | ---------------------------------------------------- |
| `endpoint`       | `http://localhost:3000`   | base URL of the loge server                          |
| `path`           | `/api/v1/push`            | push endpoint path                                   |
| `labels`         | `{}`                      | static stream labels attached to every record        |
| `level`          | `Logger::INFO`            | minimum severity                                     |
| `level_label`    | `"level"`                 | label key for the severity (`nil` disables)          |
| `formatter`      | `Loge::Formatter`         | renders `(severity, time, progname, message)` → line |
| `batch_size`     | `100`                     | records per push before a forced delivery            |
| `flush_interval` | `1.0`                     | seconds between time-based deliveries                |
| `queue_capacity` | `4096`                    | bounded queue between loggers and the sender         |
| `block_on_full`  | `false`                   | block instead of dropping when the queue is full     |
| `bearer_token`   | `nil`                     | sent as `Authorization: Bearer ...`                  |
| `max_retries`    | `2`                       | extra delivery attempts per batch                    |
| `open_timeout`   | `5`                       | connect timeout in seconds                           |
| `read_timeout`   | `30`                      | response timeout in seconds                          |
| `on_error`       | warn to stderr            | called with delivery errors and drop reports         |

By default a full queue drops records rather than blocking the caller; drops
are counted and reported through `on_error` when the client closes. The sender
keeps one HTTP connection alive across batches and restarts itself after a
`fork`, dropping inherited queue items (they are the parent's to deliver).

`on_error` must not log back through this logger, or it will feed its own
queue.

## Development

```sh
bundle install
bundle exec rspec    # or: task ruby:test (from the repo root)
```
