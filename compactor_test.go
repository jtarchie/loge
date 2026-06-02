package loge_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Compactor", func() {
	var outputPath string

	BeforeEach(func() {
		outputPath = GinkgoT().TempDir()
	})

	makePayload := func(app, line string, ts int64) loge.Payload {
		return loge.Payload{
			Streams: loge.Streams{
				loge.Entry{
					Stream: loge.Stream{"app": app},
					Values: loge.Values{loge.Value{strconv.FormatInt(ts, 10), line}},
				},
			},
		}
	}

	It("merges flush files into one indexed segment", func() {
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		const numFiles = 5

		base := int64(1_700_000_000_000_000_000)
		for i := range numFiles {
			buckets.Append(makePayload("svc", fmt.Sprintf("line-%d alpha", i), base+int64(i)))
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(numFiles))

		Expect(buckets.Close()).To(Succeed())

		compactor := loge.NewCompactor(outputPath, 2, 128, time.Hour)
		merged, err := compactor.Compact()
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(Equal(numFiles))

		// Sources are gone, exactly one segment remains.
		bucketFiles, _ := filepath.Glob(filepath.Join(outputPath, "bucket-*.sqlite.zst"))
		Expect(bucketFiles).To(BeEmpty())

		segments, _ := filepath.Glob(filepath.Join(outputPath, "segment-*.sqlite.zst"))
		Expect(segments).To(HaveLen(1))

		dbClient, err := sql.Open("sqlite3", segments[0]+"?vfs=zstd")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = dbClient.Close() }()

		// All stream rows survived the merge.
		var streamCount int
		err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&streamCount)
		Expect(err).NotTo(HaveOccurred())
		Expect(streamCount).To(Equal(numFiles))

		// Label ids did not collide across files.
		var labelCount int
		err = dbClient.QueryRow("SELECT COUNT(DISTINCT id) FROM labels").Scan(&labelCount)
		Expect(err).NotTo(HaveOccurred())
		Expect(labelCount).To(Equal(numFiles))

		// The trigram line index is queryable and references every line.
		var matched int
		err = dbClient.QueryRow("SELECT COUNT(*) FROM line_search WHERE line_search MATCH 'alpha'").Scan(&matched)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(Equal(numFiles))

		// Segment time bounds span the merged data.
		var minTimestamp, maxTimestamp int64
		Expect(dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'minTimestamp'").Scan(&minTimestamp)).To(Succeed())
		Expect(dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'maxTimestamp'").Scan(&maxTimestamp)).To(Succeed())
		Expect(minTimestamp).To(Equal(base))
		Expect(maxTimestamp).To(Equal(base + numFiles - 1))
	})

	It("does nothing when there are fewer files than the threshold", func() {
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		buckets.Append(makePayload("svc", "only one", 1_700_000_000_000_000_000))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(1))

		Expect(buckets.Close()).To(Succeed())

		compactor := loge.NewCompactor(outputPath, 5, 128, time.Hour)
		merged, err := compactor.Compact()
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(Equal(0))
	})
})
