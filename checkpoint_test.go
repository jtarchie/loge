package loge_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WAL checkpoint", func() {
	It("prunes the write-ahead log once data is durably flushed", func() {
		outputPath := GinkgoT().TempDir()
		walDir := filepath.Join(outputPath, "wal")

		wal, err := loge.OpenWAL(walDir)
		Expect(err).NotTo(HaveOccurred())

		checkpointer := loge.NewCheckpointer(wal, 50*time.Millisecond)

		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false,
			loge.WithWAL(wal),
			loge.WithDurableReport(checkpointer.Report),
			loge.WithFlushInterval(100*time.Millisecond),
		)
		Expect(err).NotTo(HaveOccurred())

		const numPayloads = 10

		base := int64(1_700_000_000_000_000_000)
		for i := range numPayloads {
			payload := loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": "svc"},
						Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(i), 10), "line"}},
					},
				},
			}
			Expect(buckets.Append(payload)).To(Succeed())
		}

		// All payloads flush to compressed segments.
		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(numPayloads))

		// Once the data is durable, the checkpointer prunes the WAL back to
		// empty rather than letting it hold the whole session.
		Eventually(func() int {
			segments, _ := filepath.Glob(filepath.Join(walDir, "wal-*.log"))

			return len(segments)
		}, "5s").Should(Equal(0))

		// The data is still present and queryable.
		matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
		Expect(err).NotTo(HaveOccurred())

		total := 0
		for _, match := range matches {
			db, err := sql.Open("sqlite3", match+"?vfs=zstd")
			Expect(err).NotTo(HaveOccurred())

			var count int
			Expect(db.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)).To(Succeed())
			total += count
			_ = db.Close()
		}
		Expect(total).To(Equal(numPayloads))

		Expect(buckets.Close()).To(Succeed())
		checkpointer.Stop()
		Expect(wal.Close()).To(Succeed())
	})
})
