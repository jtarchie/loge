package loge_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// fakeStore stands in for S3: it copies uploads into a directory served over
// HTTP by an httptest server.
type fakeStore struct {
	remoteDir string
	baseURL   string
}

func (s *fakeStore) Put(_ context.Context, key, localPath string) (string, error) {
	dst := filepath.Join(s.remoteDir, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return "", err
	}

	data, err := os.ReadFile(localPath) //nolint: gosec
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(dst, data, 0o640); err != nil {
		return "", err
	}

	return s.baseURL + "/" + key, nil
}

func (s *fakeStore) Size(_ context.Context, key string) (int64, error) {
	info, err := os.Stat(filepath.Join(s.remoteDir, key))
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

var _ = Describe("Uploader", func() {
	It("rotates a segment to remote, sweeps the local copy, and serves reads from remote", func() {
		dir := GinkgoT().TempDir()
		remoteDir := GinkgoT().TempDir()

		catalog, err := managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })

		server := httptest.NewServer(http.FileServer(http.Dir(remoteDir)))
		DeferCleanup(server.Close)

		// Produce a local, cataloged segment.
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())

		base := int64(1_700_000_000_000_000_000)
		for i := range 4 {
			buckets.Append(loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": "svc"},
						Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(i), 10), fmt.Sprintf("line %d", i)}},
					},
				},
			})
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(4))

		Expect(buckets.Close()).To(Succeed())

		_, err = loge.NewCompactor(dir, 2, 128, time.Hour, loge.WithCompactorCatalog(catalog)).Compact()
		Expect(err).NotTo(HaveOccurred())

		segments, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(HaveLen(1))
		localPath := segments[0].LocalPath
		Expect(localPath).To(BeAnExistingFile())

		// Rotate (age 0 = everything is eligible) then sweep (grace 0).
		store := &fakeStore{remoteDir: remoteDir, baseURL: server.URL}
		uploader := loge.NewUploader(dir, catalog, store,
			loge.WithRotateAge(0), loge.WithRotateGrace(0), loge.WithUploadPrefix("loge"))

		rotated, err := uploader.Rotate(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(rotated).To(Equal(1))

		// Catalog now points at the remote object.
		segments, err = catalog.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(segments[0].Location).To(Equal(managers.LocationRemote))
		Expect(segments[0].RemoteURL).To(HavePrefix(server.URL + "/loge/segment-"))

		// Sweep removes the local copy.
		Expect(uploader.Sweep()).To(Succeed())
		Expect(localPath).NotTo(BeAnExistingFile())

		segments, err = catalog.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(segments[0].LocalPath).To(BeEmpty())

		// Querying now reads the segment back over HTTP from the "bucket".
		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		results, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(4))
		Expect(results[0].Labels).To(HaveKeyWithValue("app", "svc"))
	})
})
