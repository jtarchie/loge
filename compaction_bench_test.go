package loge_test

// Benchmark for the compaction merge (compactor.merge / copyFileInto), the path a
// Fly CPU profile flagged as the #1 ingest cost once JSON decode was fixed. It
// builds a set of realistic flush files once, then times Compact() merging them
// into a single segment. Each iteration merges a fresh copy (Compact consumes the
// sources). Run: go test -run '^$' -bench BenchmarkCompactMerge -benchmem .

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
)

func buildFlushTemplate(tb testing.TB, numFiles, rowsPerFile int) ([]string, string) {
	tb.Helper()

	dir := tb.TempDir()

	b, err := loge.NewBuckets(context.Background(), 1, rowsPerFile, dir, false, loge.WithFlushInterval(time.Hour))
	if err != nil {
		tb.Fatal(err)
	}

	apps := []string{"checkout", "auth", "payments", "catalog", "search", "gateway"}
	base := int64(1_700_000_000_000_000_000)
	n := 0

	for f := 0; f < numFiles; f++ {
		for r := 0; r < rowsPerFile; r++ {
			line := fmt.Sprintf(`{"level":"info","msg":"connection refused","trace_id":"%016x","latency_ms":%d}`, n, n%2000)
			b.Append(loge.Payload{Streams: loge.Streams{loge.Entry{
				Stream: loge.Stream{"app": apps[n%len(apps)], "env": "prod"},
				Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(n), 10), line}},
			}}})
			n++
		}
	}

	if err := b.Close(); err != nil {
		tb.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))
	if len(files) < numFiles/2 {
		tb.Fatalf("expected ~%d flush files, got %d", numFiles, len(files))
	}

	return files, dir
}

func copyFile(tb testing.TB, src, dst string) {
	tb.Helper()

	in, err := os.Open(src)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		tb.Fatal(err)
	}
}

func BenchmarkCompactMerge(b *testing.B) {
	files, _ := buildFlushTemplate(b, 16, 2000) // 16 files x 2000 rows = 32k rows merged

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		for _, f := range files {
			copyFile(b, f, filepath.Join(dir, filepath.Base(f)))
		}
		b.StartTimer()

		c := loge.NewCompactor(dir, 2, 128, time.Hour)
		merged, err := c.Compact()
		if err != nil {
			b.Fatal(err)
		}
		if merged != len(files) {
			b.Fatalf("merged %d files, want %d", merged, len(files))
		}
	}
}
