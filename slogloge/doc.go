// Package slogloge provides a log/slog Handler that ships structured logs to a
// loge server's /api/v1/push endpoint.
//
// Each record is rendered as a JSON line (reusing the standard library's
// slog.JSONHandler) and sent as the push value's line, with a configurable set
// of low-cardinality stream labels plus the record's level. Delivery is
// asynchronous and batched on a background goroutine, so logging never blocks on
// the network; call Close on shutdown to flush.
//
// Basic usage:
//
//	handler := slogloge.New(
//		slogloge.WithEndpoint("http://localhost:3000"),
//		slogloge.WithStaticLabels(map[string]string{"app": "checkout", "env": "prod"}),
//	)
//	defer handler.Close(context.Background())
//
//	slog.SetDefault(slog.New(handler))
//	slog.Info("user logged in", "user_id", "abc123")
//
// The package depends only on the standard library, so importing it does not
// pull in the root loge package's cgo sqlite, echo, and aws-sdk dependencies.
//
// Labels are indexed and should stay low-cardinality (app, env, host, level).
// High-cardinality data (trace_id, user_id, request paths) belongs in the log
// line, where loge treats it as a keyword-search target.
package slogloge
