package loge_test

import (
	"database/sql"
	"os"
	"path/filepath"

	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Buckets", func() {
	var outputPath string

	BeforeEach(func() {
		var err error

		outputPath, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())
	})

	When("there is one bucket", func() {
		It("only creates file at a time", func() {
			buckets, err := loge.NewBuckets(1, 1, outputPath)
			Expect(err).NotTo(HaveOccurred())
			buckets.Append(createPayload(1, 1))

			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

				return len(matches)
			}).Should(BeNumerically("==", 1), "5s")

			Consistently(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

				return len(matches)
			}).Should(BeNumerically("==", 1))
		})

		It("creates data that can be searched", func() {
			payload := createPayload(1, 1)
			buckets, err := loge.NewBuckets(1, 1, outputPath)
			Expect(err).NotTo(HaveOccurred())
			buckets.Append(payload)

			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

				return len(matches)
			}).Should(BeNumerically("==", 1), "5s")

			matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
			Expect(err).NotTo(HaveOccurred())

			sqliteFilename := matches[0]
			dbClient, err := sql.Open("sqlite3", sqliteFilename+"?vfs=zstd")
			Expect(err).NotTo(HaveOccurred())

			var count int

			err = dbClient.QueryRow("SELECT COUNT(*) FROM labels").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(BeNumerically(">=", 1))

			err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(BeNumerically(">=", 1))

			err = dbClient.QueryRow("SELECT COUNT(*) FROM search WHERE search MATCH 'tag'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(BeNumerically(">=", 1))

			var value string
			err = dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'minTimestamp'").Scan(&value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(payload.Streams[0].Values[0][0]))

			err = dbClient.QueryRow("SELECT value FROM metadata WHERE key = 'maxTimestamp'").Scan(&value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(payload.Streams[0].Values[0][0]))
		})
	})

	DescribeTable("When creating files", func(bucketSize, payloadSize, values, expectedFiles int) {
		buckets, err := loge.NewBuckets(bucketSize, payloadSize, outputPath)
		Expect(err).NotTo(HaveOccurred())

		for range values {
			buckets.Append(createPayload(1, 1))
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically("==", expectedFiles), "10s")

		Consistently(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically("==", expectedFiles))
	},
		Entry("multiple buckets, small payload", 2, 1, 1, 1),
		Entry("one bucket, larger payload", 1, 2, 2, 1),
		Entry("multiple buckets, larger payload", 2, 2, 4, 2),
		Entry("lots of things", 10, 1_000, 10_000, 10),
	)
})
