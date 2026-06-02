package loge_test

import (
	"os"
	"path/filepath"

	"github.com/jtarchie/loge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WAL", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	makePayload := func(line string) *loge.Payload {
		return &loge.Payload{
			Streams: loge.Streams{
				loge.Entry{
					Stream: loge.Stream{"app": "x"},
					Values: loge.Values{loge.Value{"1700000000000000000", line}},
				},
			},
		}
	}

	replayLines := func() []string {
		var lines []string

		_, err := loge.ReplayWAL(dir, func(p *loge.Payload) {
			lines = append(lines, p.Streams[0].Values[0][1])
		})
		Expect(err).NotTo(HaveOccurred())

		return lines
	}

	appendOK := func(wal *loge.WAL, line string) uint64 {
		seq, err := wal.Append(makePayload(line))
		Expect(err).NotTo(HaveOccurred())

		return seq
	}

	It("round-trips appended payloads in order with increasing sequences", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())

		Expect(appendOK(wal, "one")).To(Equal(uint64(1)))
		Expect(appendOK(wal, "two")).To(Equal(uint64(2)))
		Expect(wal.Close()).To(Succeed())

		Expect(replayLines()).To(Equal([]string{"one", "two"}))
	})

	It("stops at a torn tail without error and keeps intact records", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())
		appendOK(wal, "intact")
		Expect(wal.Close()).To(Succeed())

		segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(HaveLen(1))

		// Simulate a crash mid-write: a length header with no body/checksum.
		file, err := os.OpenFile(segments[0], os.O_WRONLY|os.O_APPEND, 0o640)
		Expect(err).NotTo(HaveOccurred())
		_, err = file.Write([]byte{0x00, 0x00, 0x03, 0xE8}) // claims 1000 bytes
		Expect(err).NotTo(HaveOccurred())
		Expect(file.Close()).To(Succeed())

		Expect(replayLines()).To(Equal([]string{"intact"}))
	})

	It("deletes sealed segments once their sequences are marked durable", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = wal.Close() }()

		// First sealed segment covers seq 1..2.
		appendOK(wal, "a")
		appendOK(wal, "b")
		wal.Rotate()

		// Second sealed segment covers seq 3.
		appendOK(wal, "c")
		wal.Rotate()

		segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(len(segments)).To(BeNumerically(">=", 2))

		// Marking seq 3 alone must NOT delete anything: the watermark cannot
		// pass the gap at seq 1/2.
		Expect(wal.MarkDurable([]uint64{3})).To(Succeed())
		before, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(len(before)).To(BeNumerically(">=", 2))

		// Now mark 1 and 2: watermark advances to 3, both sealed segments drop.
		Expect(wal.MarkDurable([]uint64{1, 2})).To(Succeed())
		after, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(after).To(BeEmpty())
	})

	It("removes all segments on RemoveWAL", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())
		appendOK(wal, "x")
		Expect(wal.Close()).To(Succeed())

		Expect(loge.RemoveWAL(dir)).To(Succeed())

		segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(BeEmpty())
	})
})
