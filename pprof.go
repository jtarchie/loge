package loge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof" //nolint:gosec // opt-in, loopback-only debug listener
	"runtime"
)

// startPprofServer serves net/http/pprof on 127.0.0.1:<port>. It is opt-in
// (--pprof-port) and loopback-only because heap profiles can expose log data.
func startPprofServer(ctx context.Context, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index) // also dispatches heap/allocs/goroutine/mutex/block
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{ //nolint:gosec // no read timeout: profile endpoints long-poll by design
		Addr:    fmt.Sprintf("127.0.0.1:%d", port),
		Handler: mux,
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("pprof server exited", slog.String("error", err.Error()))
		}
	}()

	context.AfterFunc(ctx, func() { _ = server.Close() })
}

// setProfileRates enables block/mutex contention sampling; both default off
// (rate 0) because they add a small per-event cost.
func setProfileRates(blockRate, mutexFraction int) {
	if blockRate > 0 {
		runtime.SetBlockProfileRate(blockRate)
	}

	if mutexFraction > 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
	}
}
