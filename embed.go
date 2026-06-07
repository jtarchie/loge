package loge

import "embed"

// webDist holds the esbuild-bundled web UI (built from web/ into webdist/ by
// `task web` / `npm run build:web`). It is committed so `go build`/`go install`
// always compile without a Node toolchain. The directory is named "webdist"
// rather than "dist" because .gitignore ignores any "dist" directory, which
// would exclude the bundle and break this embed.
//
//go:embed all:webdist
var webDist embed.FS
