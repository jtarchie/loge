package loge_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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

	return s.ReadURL(key), nil
}

func (s *fakeStore) Size(_ context.Context, key string) (int64, error) {
	info, err := os.Stat(filepath.Join(s.remoteDir, key))
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (s *fakeStore) ReadURL(key string) string {
	return s.baseURL + "/" + key
}

func (s *fakeStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string

	err := filepath.WalkDir(s.remoteDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(s.remoteDir, p)
		if err != nil {
			return err
		}

		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefix) {
			keys = append(keys, rel)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

// tableExists reports whether a compressed segment (.sqlite.zst, opened via the
// read-only zstd VFS) contains an object (table or index) with the given name.
func tableExists(zstPath, name string) bool {
	db, err := sql.Open("sqlite3", zstPath+"?vfs=zstd")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = db.Close() }()

	var count int
	Expect(db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, name).Scan(&count)).To(Succeed())

	return count > 0
}

// rowCount returns the number of rows in a table of a compressed segment.
func rowCount(zstPath, table string) int {
	db, err := sql.Open("sqlite3", zstPath+"?vfs=zstd")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = db.Close() }()

	var count int
	Expect(db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count)).To(Succeed())

	return count
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

	It("uploads an FTS-stripped cold copy that still serves keyword (LIKE) queries", func() {
		dir := GinkgoT().TempDir()
		remoteDir := GinkgoT().TempDir()

		catalog, err := managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })

		server := httptest.NewServer(http.FileServer(http.Dir(remoteDir)))
		DeferCleanup(server.Close)

		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())

		base := int64(1_700_000_000_000_000_000)
		for i := range 4 {
			buckets.Append(loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": "svc"},
						Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(i), 10), fmt.Sprintf("line %d alpha", i)}},
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
		segmentID := segments[0].ID

		// The local (hot) segment keeps its FTS index.
		localPath := segments[0].LocalPath
		Expect(tableExists(localPath, "line_search")).To(BeTrue(), "local segment keeps FTS")

		store := &fakeStore{remoteDir: remoteDir, baseURL: server.URL}
		uploader := loge.NewUploader(dir, catalog, store,
			loge.WithRotateAge(0), loge.WithRotateGrace(0), loge.WithUploadPrefix("loge"))

		rotated, err := uploader.Rotate(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(rotated).To(Equal(1))

		// The uploaded cold copy has NO FTS index, but keeps the data, the
		// timestamp index, and the metadata bounds.
		remotePath := filepath.Join(remoteDir, "loge", segmentID)
		Expect(remotePath).To(BeAnExistingFile())
		Expect(tableExists(remotePath, "line_search")).To(BeFalse(), "cold copy must be FTS-stripped")
		Expect(tableExists(remotePath, "idx_streams_timestamp")).To(BeTrue(), "cold copy keeps the timestamp index")
		Expect(rowCount(remotePath, "streams")).To(Equal(4))
		Expect(rowCount(remotePath, "labels")).To(Equal(4))

		// No temp strip artifacts are left behind in the working directory.
		leftovers, _ := filepath.Glob(filepath.Join(dir, "*.strip*"))
		Expect(leftovers).To(BeEmpty())

		Expect(uploader.Sweep()).To(Succeed())

		// A keyword query now resolves over HTTP against the stripped copy, using
		// LIKE only (no FTS) — and still returns the right line.
		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		results, err := manager.Query(context.Background(), managers.QueryRequest{Line: "line 2"})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Line).To(Equal("line 2 alpha"))
	})

	It("rebuilds remote catalog rows from an S3 listing on a fresh catalog", func() {
		dir := GinkgoT().TempDir()
		remoteDir := GinkgoT().TempDir()

		// Count HTTP reads so we can prove the rebuild opens no segment.
		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.FileServer(http.Dir(remoteDir)).ServeHTTP(w, r)
		}))
		DeferCleanup(server.Close)

		store := &fakeStore{remoteDir: remoteDir, baseURL: server.URL}
		base := int64(1_700_000_000_000_000_000)

		// Produce a local segment and rotate it to remote, then sweep the local copy.
		catalog, err := managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())

		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())

		for i := range 4 {
			Expect(buckets.Append(loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": "svc"},
						Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(i), 10), fmt.Sprintf("line %d", i)}},
					},
				},
			})).To(Succeed())
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(4))
		Expect(buckets.Close()).To(Succeed())

		_, err = loge.NewCompactor(dir, 2, 128, time.Hour, loge.WithCompactorCatalog(catalog)).Compact()
		Expect(err).NotTo(HaveOccurred())

		uploader := loge.NewUploader(dir, catalog, store,
			loge.WithRotateAge(0), loge.WithRotateGrace(0), loge.WithUploadPrefix("loge"))
		rotated, err := uploader.Rotate(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(rotated).To(Equal(1))
		Expect(uploader.Sweep()).To(Succeed())

		original, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(original).To(HaveLen(1))

		// Lose the catalog entirely (fresh node / lost disk).
		Expect(catalog.Close()).To(Succeed())
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(filepath.Join(dir, "catalog.sqlite"+suffix))
		}

		// A fresh catalog: local reconcile finds nothing (the local copy was swept).
		fresh, err := managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = fresh.Close() })
		Expect(fresh.Reconcile(dir)).To(Succeed())

		empty, err := fresh.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(empty).To(BeEmpty())

		// Rebuilding from the S3 listing rediscovers the remote segment, parsing
		// its bounds from the filename WITHOUT opening it over HTTP.
		before := requests.Load()
		rebuilt := loge.NewUploader(dir, fresh, store, loge.WithUploadPrefix("loge"))
		added, err := rebuilt.ReconcileRemote(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(1))
		Expect(requests.Load()).To(Equal(before), "reconcile must not open any segment over HTTP")

		segments, err := fresh.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(HaveLen(1))
		Expect(segments[0].Location).To(Equal(managers.LocationRemote))
		Expect(segments[0].ID).To(Equal(original[0].ID))
		Expect(segments[0].MinTimestamp).To(Equal(base))
		Expect(segments[0].MaxTimestamp).To(Equal(base + 3))

		// The rebuilt catalog can serve the segment back over HTTP.
		manager, err := managers.NewLocal(dir, managers.WithCatalog(fresh))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		results, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(4))
	})
})
