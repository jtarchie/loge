package loge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/labstack/echo/v5"
	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ginkgo is imported qualified (not dot-imported) because this internal test
// lives in package loge, whose Entry type would collide with ginkgo's Entry.

var _ = ginkgo.Describe("client-side search internals", func() {
	ginkgo.Describe("presignTransport", func() {
		ginkgo.It("re-attaches the presigned query for a known canonical url", func() {
			var gotQuery string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotQuery = r.URL.RawQuery
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			transport := &presignTransport{base: http.DefaultTransport}
			canonical := server.URL + "/segment-1.sqlite.zst"
			transport.put(canonical, "X-Amz-Signature=abc123&X-Amz-Expires=3600")

			req, err := http.NewRequest(http.MethodGet, canonical, nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := transport.RoundTrip(req)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			Expect(gotQuery).To(Equal("X-Amz-Signature=abc123&X-Amz-Expires=3600"))
			// The caller's (cached, path-based) request is left untouched.
			Expect(req.URL.RawQuery).To(BeEmpty())
		})

		ginkgo.It("passes through requests for unknown urls unchanged", func() {
			var gotQuery string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotQuery = r.URL.RawQuery
			}))
			defer server.Close()

			transport := &presignTransport{base: http.DefaultTransport}

			req, err := http.NewRequest(http.MethodGet, server.URL+"/unknown?keep=1", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := transport.RoundTrip(req)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			Expect(gotQuery).To(Equal("keep=1"))
		})
	})

	ginkgo.Describe("bearerAuth", func() {
		newRouter := func(apiKey string) *echo.Echo {
			e := echo.New()
			e.POST("/guarded", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			}, bearerAuth(apiKey))

			return e
		}

		hit := func(e *echo.Echo, auth string) int {
			req := httptest.NewRequest(http.MethodPost, "/guarded", nil)
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			return rec.Code
		}

		ginkgo.It("allows any request when no key is configured", func() {
			Expect(hit(newRouter(""), "")).To(Equal(http.StatusOK))
		})

		ginkgo.It("rejects a missing, prefixless, or wrong bearer token", func() {
			e := newRouter("s3cr3t")
			Expect(hit(e, "")).To(Equal(http.StatusUnauthorized))
			Expect(hit(e, "Bearer nope")).To(Equal(http.StatusUnauthorized))
			Expect(hit(e, "s3cr3t")).To(Equal(http.StatusUnauthorized))
		})

		ginkgo.It("accepts the correct bearer token", func() {
			Expect(hit(newRouter("s3cr3t"), "Bearer s3cr3t")).To(Equal(http.StatusOK))
		})
	})

	ginkgo.Describe("S3Store.Presign", func() {
		ginkgo.It("produces a signed GET url for the key", func() {
			ginkgo.GinkgoT().Setenv("AWS_ACCESS_KEY_ID", "test")
			ginkgo.GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", "secret")

			store, err := NewS3Store(context.Background(), S3Config{
				Bucket:         "my-bucket",
				Endpoint:       "https://s3.example.com",
				Region:         "us-east-1",
				ForcePathStyle: true,
			})
			Expect(err).NotTo(HaveOccurred())

			url, err := store.Presign(context.Background(), "loge/segment-1.sqlite.zst", time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(url).To(ContainSubstring("my-bucket/loge/segment-1.sqlite.zst"))
			Expect(url).To(ContainSubstring("X-Amz-Signature="))
			Expect(url).To(ContainSubstring("X-Amz-Expires=3600"))
		})
	})
})
