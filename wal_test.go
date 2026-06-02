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

	It("round-trips appended payloads in order", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())

		Expect(wal.Append(makePayload("one"))).To(Succeed())
		Expect(wal.Append(makePayload("two"))).To(Succeed())
		Expect(wal.Close()).To(Succeed())

		Expect(replayLines()).To(Equal([]string{"one", "two"}))
	})

	It("stops at a torn tail without error and keeps intact records", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(wal.Append(makePayload("intact"))).To(Succeed())
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

	It("removes all segments on RemoveWAL", func() {
		wal, err := loge.OpenWAL(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(wal.Append(makePayload("x"))).To(Succeed())
		Expect(wal.Close()).To(Succeed())

		Expect(loge.RemoveWAL(dir)).To(Succeed())

		segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(BeEmpty())
	})
})
