package loge_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

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
			buckets, err := loge.NewBuckets(1, 1, outputPath, false)
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
			buckets, err := loge.NewBuckets(1, 1, outputPath, false)
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
		buckets, err := loge.NewBuckets(bucketSize, payloadSize, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		for range values {
			buckets.Append(createPayload(1, 1))
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "10s").Should(BeNumerically(">=", expectedFiles))

		Consistently(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically(">=", expectedFiles))
	},
		Entry("multiple buckets, small payload", 2, 1, 1, 1),
		Entry("one bucket, larger payload", 1, 2, 2, 1),
		Entry("multiple buckets, larger payload", 2, 2, 4, 2),
		Entry("lots of things", 10, 1_000, 10_000, 10),
	)

	Describe("Graceful Shutdown", func() {
		It("completes Close() quickly with no data", func() {
			buckets, err := loge.NewBuckets(2, 100, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Send one payload and wait a bit to ensure workers are initialized
			buckets.Append(createPayload(1, 1))
			time.Sleep(50 * time.Millisecond)

			// Close should complete almost instantly with minimal data
			err = buckets.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("completes Close() after flushing pending data", func() {
			buckets, err := loge.NewBuckets(2, 10, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Add some data (not enough to trigger automatic flush)
			for i := 0; i < 5; i++ {
				buckets.Append(createPayload(1, 1))
			}
			// Wait for workers to be fully initialized
			time.Sleep(50 * time.Millisecond)

			// Close should flush the pending data and complete
			err = buckets.Close()
			Expect(err).NotTo(HaveOccurred())

			// Verify data was written
			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
				return len(matches)
			}, "10s").Should(BeNumerically(">=", 1))
		})

		It("completes Close() under heavy load with backpressure", func() {
			// Small payload size to trigger frequent flushes and backpressure
			buckets, err := loge.NewBuckets(2, 100, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Flood with data to create backpressure
			// Use a goroutine to send data while we try to close
			sendDone := make(chan struct{})
			go func() {
				defer close(sendDone)
				for i := 0; i < 5000; i++ {
					select {
					case <-sendDone:
						return
					default:
						buckets.Append(createPayload(1, 1))
					}
				}
			}()

			// Give some time for data to queue up
			Eventually(sendDone, "5s").Should(BeClosed())

			// Now close - this should complete within a reasonable time
			closeDone := make(chan struct{})
			go func() {
				defer close(closeDone)
				_ = buckets.Close()
			}()

			// Close must complete within 30 seconds even with backpressure
			Eventually(closeDone, "30s").Should(BeClosed())
		})

		It("completes Close() when called during active writes", func() {
			buckets, err := loge.NewBuckets(2, 50, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Start continuous writes in background
			stopWrites := make(chan struct{})
			writesDone := make(chan struct{})
			go func() {
				defer close(writesDone)
				for {
					select {
					case <-stopWrites:
						return
					default:
						buckets.Append(createPayload(1, 1))
					}
				}
			}()

			// Let writes run for a bit
			time.Sleep(100 * time.Millisecond)

			// Signal to stop writes
			close(stopWrites)

			// Wait for writes goroutine to finish
			Eventually(writesDone, "1s").Should(BeClosed())

			// Close should complete
			closeDone := make(chan struct{})
			go func() {
				defer close(closeDone)
				_ = buckets.Close()
			}()

			Eventually(closeDone, "30s").Should(BeClosed())
		})

		It("does not lose data during shutdown", func() {
			buckets, err := loge.NewBuckets(1, 10, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Send exactly 25 payloads
			numPayloads := 25
			for i := 0; i < numPayloads; i++ {
				buckets.Append(createPayload(1, 1))
			}

			// Close and wait - this should flush all data
			err = buckets.Close()
			Expect(err).NotTo(HaveOccurred())

			// Wait for compression to complete (compressors run after flush)
			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
				return len(matches)
			}, "10s").Should(BeNumerically(">=", 1))

			// Count total streams in all files
			matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
			Expect(err).NotTo(HaveOccurred())

			totalStreams := 0
			for _, match := range matches {
				dbClient, err := sql.Open("sqlite3", match+"?vfs=zstd")
				Expect(err).NotTo(HaveOccurred())

				var count int
				err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				totalStreams += count
				_ = dbClient.Close()
			}

			// Each payload has 1 stream with 1 value
			Expect(totalStreams).To(Equal(numPayloads))
		})

		It("handles extreme backpressure without hanging", func() {
			// Very small payload size and buffer to maximize backpressure
			buckets, err := loge.NewBuckets(1, 5, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Send enough data to definitely cause backpressure
			numPayloads := 100
			for i := 0; i < numPayloads; i++ {
				buckets.Append(createPayload(1, 1))
			}

			// Close should complete within reasonable time
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- buckets.Close()
			}()

			// Must complete within 60 seconds - if it hangs, this will fail
			var closeErr error
			Eventually(closeDone, "60s").Should(Receive(&closeErr))
			Expect(closeErr).NotTo(HaveOccurred())

			// Wait for files to be compressed
			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
				return len(matches)
			}, "10s").Should(BeNumerically(">=", 1))

			// Verify data integrity
			matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
			Expect(err).NotTo(HaveOccurred())

			totalStreams := 0
			for _, match := range matches {
				dbClient, err := sql.Open("sqlite3", match+"?vfs=zstd")
				Expect(err).NotTo(HaveOccurred())

				var count int
				err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				totalStreams += count
				_ = dbClient.Close()
			}

			// We should have at least some data (may lose some during forced shutdown)
			Expect(totalStreams).To(BeNumerically(">=", 1))
		})
	})

	Describe("Drop on Backpressure", func() {
		It("does not block when dropOnBackpressure is enabled", func() {
			// Very small payload size to trigger backpressure quickly
			buckets, err := loge.NewBuckets(1, 5, outputPath, true)
			Expect(err).NotTo(HaveOccurred())

			// Flood with data - should not block due to drop mode
			startTime := time.Now()
			for i := 0; i < 200; i++ {
				buckets.Append(createPayload(1, 1))
			}
			appendDuration := time.Since(startTime)

			// Appends should be fast (not blocking on backpressure)
			// With blocking mode, this would take much longer
			Expect(appendDuration).To(BeNumerically("<", 5*time.Second))

			// Close should complete quickly since we drop on backpressure
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- buckets.Close()
			}()

			Eventually(closeDone, "30s").Should(Receive())
		})

		It("may lose data when dropOnBackpressure is enabled under heavy load", func() {
			// Very small settings to maximize backpressure
			buckets, err := loge.NewBuckets(1, 5, outputPath, true)
			Expect(err).NotTo(HaveOccurred())

			// Send a lot of data quickly
			numPayloads := 500
			for i := 0; i < numPayloads; i++ {
				buckets.Append(createPayload(1, 1))
			}

			// Close
			err = buckets.Close()
			Expect(err).NotTo(HaveOccurred())

			// Wait for files
			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
				return len(matches)
			}, "10s").Should(BeNumerically(">=", 1))

			// Count data - we expect some data loss
			matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
			Expect(err).NotTo(HaveOccurred())

			totalStreams := 0
			for _, match := range matches {
				dbClient, err := sql.Open("sqlite3", match+"?vfs=zstd")
				Expect(err).NotTo(HaveOccurred())

				var count int
				err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				totalStreams += count
				_ = dbClient.Close()
			}

			// Some data should be written, but likely less than all due to drops
			Expect(totalStreams).To(BeNumerically(">=", 1))
			// Note: We can't assert exact data loss since it depends on timing
		})

		It("does not lose data when dropOnBackpressure is disabled", func() {
			buckets, err := loge.NewBuckets(1, 10, outputPath, false)
			Expect(err).NotTo(HaveOccurred())

			// Send data
			numPayloads := 50
			for i := 0; i < numPayloads; i++ {
				buckets.Append(createPayload(1, 1))
			}

			// Close and wait
			err = buckets.Close()
			Expect(err).NotTo(HaveOccurred())

			// Wait for files
			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
				return len(matches)
			}, "10s").Should(BeNumerically(">=", 1))

			// Count data - should have all data
			matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))
			Expect(err).NotTo(HaveOccurred())

			totalStreams := 0
			for _, match := range matches {
				dbClient, err := sql.Open("sqlite3", match+"?vfs=zstd")
				Expect(err).NotTo(HaveOccurred())

				var count int
				err = dbClient.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				totalStreams += count
				_ = dbClient.Close()
			}

			// All data should be preserved
			Expect(totalStreams).To(Equal(numPayloads))
		})

		It("shuts down quickly with dropOnBackpressure under extreme load", func() {
			buckets, err := loge.NewBuckets(1, 5, outputPath, true)
			Expect(err).NotTo(HaveOccurred())

			// Continuously send data
			stopWrites := make(chan struct{})
			writesDone := make(chan struct{})
			go func() {
				defer close(writesDone)
				for {
					select {
					case <-stopWrites:
						return
					default:
						buckets.Append(createPayload(1, 1))
					}
				}
			}()

			// Let it run for a bit to build up backpressure
			time.Sleep(200 * time.Millisecond)
			close(stopWrites)

			// Wait for writes to stop before closing
			Eventually(writesDone, "1s").Should(BeClosed())

			// Close should be fast since we drop data
			startTime := time.Now()
			err = buckets.Close()
			closeDuration := time.Since(startTime)

			Expect(err).NotTo(HaveOccurred())
			// Should close within 10 seconds even under heavy load
			Expect(closeDuration).To(BeNumerically("<", 10*time.Second))
		})
	})
})
