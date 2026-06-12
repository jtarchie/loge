package loge_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"encoding/base64"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
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

		// Segments carry no FTS index; keyword search is a plain LIKE scan.
		var hasFTS int
		Expect(dbClient.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 'line_search'").Scan(&hasFTS)).To(Succeed())
		Expect(hasFTS).To(Equal(0), "segments must not build an FTS index")

		var matched int
		err = dbClient.QueryRow("SELECT COUNT(*) FROM streams WHERE line LIKE '%alpha%'").Scan(&matched)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(Equal(numFiles))

		// Segment time bounds span the merged data.
		var minTimestamp, maxTimestamp int64
		Expect(dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'minTimestamp'").Scan(&minTimestamp)).To(Succeed())
		Expect(dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'maxTimestamp'").Scan(&maxTimestamp)).To(Succeed())
		Expect(minTimestamp).To(Equal(base))
		Expect(maxTimestamp).To(Equal(base + numFiles - 1))
	})

	It("preserves the stream→label mapping across merged files", func() {
		// Each flush file carries a DISTINCT label, so the label-id offset remap
		// must keep every stream pointing at its own file's label. (The merge test
		// above reuses one label, which can't catch a cross-file mismatch.)
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		const numFiles = 6

		base := int64(1_700_000_000_000_000_000)
		for i := range numFiles {
			buckets.Append(makePayload(fmt.Sprintf("app-%d", i), fmt.Sprintf("line-%d", i), base+int64(i)))
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

		segments, _ := filepath.Glob(filepath.Join(outputPath, "segment-*.sqlite.zst"))
		Expect(segments).To(HaveLen(1))

		dbClient, err := sql.Open("sqlite3", segments[0]+"?vfs=zstd")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = dbClient.Close() }()

		// Join streams back to labels and verify line-i still resolves to app-i.
		rows, err := dbClient.Query(`
			SELECT s.line, json_extract(l.payload, '$.app')
			FROM streams s JOIN labels l ON s.label_id = l.id`)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = rows.Close() }()

		got := map[string]string{}
		for rows.Next() {
			var line, app string
			Expect(rows.Scan(&line, &app)).To(Succeed())
			got[line] = app
		}
		Expect(rows.Err()).NotTo(HaveOccurred())

		Expect(got).To(HaveLen(numFiles))
		for i := range numFiles {
			Expect(got[fmt.Sprintf("line-%d", i)]).To(Equal(fmt.Sprintf("app-%d", i)),
				"line-%d must still map to app-%d after the offset remap", i, i)
		}
	})

	It("builds a correct line filter from the unioned per-flush hashes", func() {
		// The filter is built by unioning each flush's stored trigram hashes (not by
		// rescanning lines). Verify it prunes soundly: keywords present in the data
		// must read as "may contain" (a false negative would drop query results), and
		// a keyword absent from every line must read as "provably absent".
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		const numFiles = 5

		base := int64(1_700_000_000_000_000_000)
		present := []string{"connection refused", "payment authorized", "checkout"}
		for i := range numFiles {
			buckets.Append(makePayload("svc", fmt.Sprintf("%s req-%d via /checkout", present[i%len(present)], i), base+int64(i)))
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(numFiles))

		Expect(buckets.Close()).To(Succeed())

		merged, err := loge.NewCompactor(outputPath, 2, 128, time.Hour).Compact()
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(Equal(numFiles))

		segments, _ := filepath.Glob(filepath.Join(outputPath, "segment-*.sqlite.zst"))
		Expect(segments).To(HaveLen(1))

		dbClient, err := sql.Open("sqlite3", segments[0]+"?vfs=zstd")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = dbClient.Close() }()

		var encoded string
		Expect(dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'lineFilter'").Scan(&encoded)).To(Succeed())
		filter, err := base64.StdEncoding.DecodeString(encoded)
		Expect(err).NotTo(HaveOccurred())
		Expect(filter).NotTo(BeEmpty(), "a non-empty segment must carry a line filter")

		// Every keyword that appears in the lines must NOT be pruned.
		for _, kw := range append(present, "/checkout") {
			Expect(managers.LineFilterMayContain(filter, kw)).To(BeTrue(),
				"present keyword %q must read as may-contain (no false negative)", kw)
		}

		// A keyword whose trigrams never appear must be prunable.
		Expect(managers.LineFilterMayContain(filter, "zqxjwk")).To(BeFalse(),
			"absent keyword must be provably absent (else the filter prunes nothing)")
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
