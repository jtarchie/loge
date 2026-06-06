package loge_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("loge search --local", func() {
	It("scans cold segments locally and merges with the server's hot results", func() {
		dir := GinkgoT().TempDir()

		// Build a real cold segment on disk to be served back over HTTP.
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())

		base := int64(1_700_000_000_000_000_000)
		for i := range 3 {
			buckets.Append(loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": "web"},
						Values: loge.Values{loge.Value{strconv.FormatInt(base+int64(i), 10), fmt.Sprintf("cold line %d", i)}},
					},
				},
			})
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(3))

		Expect(buckets.Close()).To(Succeed())

		_, err = loge.NewCompactor(dir, 2, 128, time.Hour).Compact()
		Expect(err).NotTo(HaveOccurred())

		segMatches, err := filepath.Glob(filepath.Join(dir, "segment-*.sqlite.zst"))
		Expect(err).NotTo(HaveOccurred())
		Expect(segMatches).To(HaveLen(1))

		segID := filepath.Base(segMatches[0])

		// One server plays both roles: the plan endpoint (returning a hot row plus
		// the cold segment) and the file server for the segment bytes. The segment
		// URL has no query string, standing in for an already-public/presigned URL.
		files := http.FileServer(http.Dir(dir))

		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/v1/search/plan" {
				_ = json.NewEncoder(w).Encode(loge.PlanResponse{
					Status: "success",
					Hot: []managers.QueryEntry{{
						Timestamp: base + 1_000_000, // newer than the cold rows
						Line:      "hot line",
						Labels:    map[string]string{"app": "web"},
					}},
					Segments:  []loge.PlanSegment{{ID: segID, URL: server.URL + "/" + segID}},
					ExpiresAt: 0,
				})

				return
			}

			files.ServeHTTP(w, r)
		}))
		DeferCleanup(server.Close)

		buf := &strings.Builder{}
		cmd := &loge.SearchCmd{
			Selector:       `{app="web"}`,
			Addr:           server.URL,
			Local:          true,
			APIKey:         "ignored-by-stub",
			Limit:          100,
			Concurrency:    4,
			FrameCacheSize: 512,
			Output:         "json",
			Out:            buf,
		}
		Expect(cmd.Run()).To(Succeed())

		var entries []managers.QueryEntry
		Expect(json.Unmarshal([]byte(buf.String()), &entries)).To(Succeed())

		// One hot row + three cold rows, newest-first (the hot row leads).
		Expect(entries).To(HaveLen(4))
		Expect(entries[0].Line).To(Equal("hot line"))
		Expect(buf.String()).To(ContainSubstring("cold line 0"))
		Expect(buf.String()).To(ContainSubstring("cold line 2"))
	})
})
