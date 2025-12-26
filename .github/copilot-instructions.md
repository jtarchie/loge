# Loge - Copilot Instructions

## Project Overview

**Purpose:** Loge is a log capture system that stores logs in SQLite files and
provides querying capabilities within timespans. The project is under active
development with ongoing measurement and refactoring cycles.

**Type:** Go HTTP server application with JavaScript benchmarking tools
**Language:** Go 1.24+ (using fts5 tag for full-text search) **Size:** Small
codebase (~15 Go files, 3 benchmark scripts)

## Critical Build Requirements

### Required Tools & Versions

- **Go:** 1.25.5+ (module requires go 1.24.0)
- **Task (go-task):** 3.45.5+ - Build automation tool
- **golangci-lint:** 2.7.2+ - Linter (10m timeout configured)
- **Deno:** 2.6.2+ - Used for formatting README.md
- **ginkgo:** v2 - Test framework (github.com/onsi/ginkgo/v2)
- **k6:** Required for benchmarks (optional for development)

### SQLite Build Tag

**CRITICAL:** All Go commands MUST include `-tags fts5` to enable full-text
search support:

```bash
go build -tags fts5 ...
go test -tags fts5 ...
go run -tags fts5 ...
```

## Build & Validation Workflow

### Standard Development Workflow

Run the default task to perform full validation:

```bash
task
```

This executes in order:

1. `go generate ./...` - Generates msgpack serialization code
2. `task format` - Formats code with deno and gofmt
3. `task lint` - Runs golangci-lint with --fix and 10m timeout
4. `task test` - Runs all tests with coverage and race detection

**Expected duration:** ~15-20 seconds for full suite **Expected warnings:** ld
warnings about malformed LC_DYSYMTAB are normal and can be ignored

### Individual Tasks

**Format Code:**

```bash
task format
```

Runs: `deno fmt README.md` then `gofmt -w .`

**Lint Code:**

```bash
task lint
```

Runs: `golangci-lint run --fix --timeout "10m"`

- Auto-fixes issues when possible
- Must complete without errors for valid code

**Run Tests:**

```bash
task test
```

Runs: `go run github.com/onsi/ginkgo/v2/ginkgo -tags fts5 -cover -race -r`

- Uses Ginkgo v2 test framework
- Runs with race detector and coverage
- Tests 3 suites: Loge Suite, FileWatcher Suite, Managers Suite
- Expected coverage: ~26% composite
- Tests start HTTP servers on random ports (output shows "http server started on
  [::]:<port>")

**Build Binary:**

```bash
go build -tags fts5 -o loge ./loge/main.go
```

**Run Server:**

```bash
task server
# Or manually:
go run -tags fts5 loge/main.go --port 6500 --payload-size 100000 --buckets 2
```

**Run Benchmarks:**

```bash
task bench
```

Runs Go benchmarks, starts server, waits for it, then runs k6 benchmarks

### Code Generation

**Always run before building/testing after modifying stream or label types:**

```bash
go generate ./...
```

Generates msgpack serialization code for:

- `streams.go` → `streams_gen.go`
- `labels.go` → `labels_gen.go`

## Project Architecture

### Directory Structure

```
/Users/jtarchie/workspace/loge/
├── loge/                   # Main entry point
│   └── main.go            # CLI entry point using kong
├── managers/              # Storage managers
│   ├── local.go           # Local file management with LRU cache
│   ├── local_test.go
│   └── managers_suite_test.go
├── file_watcher/          # File system monitoring
│   ├── file_watcher.go    # Watches for .sqlite.zst files
│   └── file_watcher_suite_test.go
├── benchmark/             # k6 performance tests
│   ├── msgpack.js
│   ├── streams.json.js
│   └── streams.msgpack.js
├── tmp/                   # Runtime SQLite files (.sqlite.zst)
├── Taskfile.yml          # Build automation (task runner)
├── go.mod                # Go dependencies
└── package.json          # Node dependencies (for benchmarks)
```

### Core Components

**Main Application Files:**

- [../cli.go](../cli.go) - CLI struct and HTTP server setup using Echo framework
- [../bucket.go](../bucket.go) - Bucket system for batching and compressing logs
- [../streams.go](../streams.go) - Stream/payload data structures with msgpack
- [../bind.go](../bind.go) - Request binding (JSON/msgpack support)
- [../marshal.go](../marshal.go) - Custom JSON serialization
- [../json.go](../json.go) - JSON serializer implementation for Echo

**Key Patterns:**

- Uses Echo v4 web framework for HTTP server
- Logs stored in compressed SQLite files (.sqlite.zst using zstd-seekable)
- Worker pools for parallel compression and flushing
- LRU cache (10 DBs) in managers for database connections
- File watcher monitors for new SQLite files matching `.sqlite.zst$`
- Supports both JSON and MessagePack payload formats

**Test Framework:**

- Uses Ginkgo v2 BDD-style tests with Gomega matchers
- Test suites: `*_suite_test.go` files initialize Ginkgo
- Tests use `_test` package suffix for black-box testing

### API Endpoints

- `POST /api/v1/push` - Accepts log streams (JSON or MessagePack)

### Configuration

- No `.golangci.yml` - uses golangci-lint defaults with 10m timeout
- No `.github/workflows` - no CI configuration currently
- Output directory for SQLite files: configurable via `--output-path` (default:
  `tmp/`)

## Common Pitfalls & Gotchas

### 1. Missing `-tags fts5`

**Problem:** Build/test failures or missing full-text search functionality
**Solution:** Always include `-tags fts5` in all go commands

### 2. ld warnings during tests

**Expected:** `ld: warning: '...LC_DYSYMTAB...'` warnings during test
compilation are normal and harmless - tests will still pass

### 3. Generated files out of sync

**Problem:** Build errors after modifying stream/label structures **Solution:**
Run `go generate ./...` before building/testing

### 4. Port conflicts in tests

Tests start HTTP servers on random ports. If tests fail with "address already in
use", a previous test may not have cleaned up properly. Retry or restart.

### 5. Task command not found

Task is a separate tool (go-task), not a Go command. Install via:
`brew install go-task` (macOS) or see https://taskfile.dev

### 6. Benchmark dependencies

k6 and node modules are only needed for `task bench`, not for regular
development.

## Validation Checklist

Before submitting changes:

1. ✅ Run `go generate ./...` if stream/label types changed
2. ✅ Run `task` (or `task format && task lint && task test`)
3. ✅ All tests pass (exit code 0)
4. ✅ No new lint issues introduced
5. ✅ Composite coverage should be ~26% or better

## Dependencies

**Key Go Dependencies:**

- `github.com/labstack/echo/v4` - HTTP framework
- `github.com/mattn/go-sqlite3` - SQLite driver (requires CGO)
- `github.com/klauspost/compress` - Zstd compression
- `github.com/SaveTheRbtz/zstd-seekable-format-go` - Seekable zstd format
- `github.com/onsi/ginkgo/v2` & `github.com/onsi/gomega` - Testing
- `github.com/tinylib/msgp` - MessagePack code generation
- `github.com/alecthomas/kong` - CLI parsing
- `github.com/fsnotify/fsnotify` - File watching
- `github.com/hashicorp/golang-lru/v2` - LRU cache

## Quick Reference

**Start developing:** `task` (runs full validation) **Run server locally:**
`task server` or `go run -tags fts5 loge/main.go --port 3000` **Run specific
test:** `go run github.com/onsi/ginkgo/v2/ginkgo -tags fts5 -focus="test name"`
**Clean build artifacts:** `rm -rf tmp/` (removes generated SQLite files) **Get
help:** `go run -tags fts5 loge/main.go --help`

## Trust These Instructions

These instructions have been validated by running all commands and examining all
build/test outputs. Only perform additional searches if information here proves
incomplete or incorrect. The project structure is small enough that these
instructions cover all essential development workflows.
