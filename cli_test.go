package loge_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/alecthomas/kong"
	"github.com/imroc/req/v3"
	"github.com/jaswdr/faker/v2"
	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/phayes/freeport"
	"github.com/samber/lo"
	"github.com/tinylib/msgp/msgp"
)

func StartCLI(args ...string) {
	command := &loge.CLI{}
	parser, err := kong.New(command)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		defer GinkgoRecover()

		ctx, err := parser.Parse(args)
		Expect(err).NotTo(HaveOccurred())

		err = ctx.Run()
		Expect(err).NotTo(HaveOccurred())
	}()

	// give a change for the HTTP server to start
	runtime.Gosched()
}

var _ = Describe("Running the application", func() {
	It("accepts a JSON payload", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		payload := generatePayload()

		httpClient := req.C()

		Eventually(func() error {
			_, err := httpClient.R().
				SetRetryCount(3).
				SetBodyJsonMarshal(payload).
				Post(fmt.Sprintf("http://localhost:%d/api/v1/push", port))

			return err
		}).ShouldNot(HaveOccurred())

		Consistently(func() int {
			response, _ := httpClient.R().
				SetRetryCount(3).
				SetBodyJsonMarshal(payload).
				Post(fmt.Sprintf("http://localhost:%d/api/v1/push", port))

			return response.StatusCode
		}).Should(Equal(http.StatusOK))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically(">=", 1), "5s")

		var labelResponse loge.LabelResponse
		_, err = httpClient.R().
			SetRetryCount(3).
			SetSuccessResult(&labelResponse).
			Get(fmt.Sprintf("http://localhost:%d/api/v1/labels", port))

		Expect(err).NotTo(HaveOccurred())
		Expect(labelResponse.Status).To(Equal("success"))

		knownLabels := lo.FlatMap(payload.Streams, func(entry loge.Entry, _ int) []string {
			return lo.Keys(entry.Stream)
		})
		Expect(labelResponse.Data).To(ConsistOf(knownLabels))
	})

	It("rejects an invalid JSON payload with 400", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		httpClient := req.C()

		Eventually(func() int {
			response, _ := httpClient.R().
				SetRetryCount(3).
				SetContentType("application/json").
				SetBodyString(`{"streams":[]}`).
				Post(fmt.Sprintf("http://localhost:%d/api/v1/push", port))

			return response.StatusCode
		}).Should(Equal(http.StatusBadRequest))
	})

	It("accepts a MsgPack payload", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		payload := generatePayload()

		contents := &bytes.Buffer{}
		err = msgp.Encode(contents, payload)
		Expect(err).NotTo(HaveOccurred())

		httpClient := req.C()

		Eventually(func() error {
			_, err := httpClient.R().
				SetRetryCount(3).
				SetBodyJsonMarshal(payload).
				Post(fmt.Sprintf("http://localhost:%d/api/v1/push", port))

			return err
		}).ShouldNot(HaveOccurred())

		Consistently(func() int {
			response, _ := httpClient.R().
				SetRetryCount(3).
				SetContentType("application/msgpack").
				SetBodyBytes(contents.Bytes()).
				Post(fmt.Sprintf("http://localhost:%d/api/v1/push", port))

			return response.StatusCode
		}).Should(Equal(http.StatusOK))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically(">=", 1), "5s")

		var labelResponse loge.LabelResponse
		response, err := httpClient.R().
			SetRetryCount(3).
			SetHeader("Accept", "application/msgpack").
			Get(fmt.Sprintf("http://localhost:%d/api/v1/labels", port))
		Expect(err).NotTo(HaveOccurred())

		err = msgp.Decode(response.Body, &labelResponse)
		Expect(err).NotTo(HaveOccurred())
		Expect(labelResponse.Status).To(Equal("success"))

		knownLabels := lo.FlatMap(payload.Streams, func(entry loge.Entry, _ int) []string {
			return lo.Keys(entry.Stream)
		})
		Expect(labelResponse.Data).To(ConsistOf(knownLabels))
	})

	It("queries pushed logs by label matcher", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		httpClient := req.C()
		pushURL := fmt.Sprintf("http://localhost:%d/api/v1/push", port)
		queryURL := fmt.Sprintf("http://localhost:%d/api/v1/query", port)

		base := int64(1_700_000_000_000_000_000)
		push := func(app, line string, ts int64) {
			payload := loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": app},
						Values: loge.Values{loge.Value{strconv.FormatInt(ts, 10), line}},
					},
				},
			}

			Eventually(func() int {
				response, _ := httpClient.R().SetRetryCount(3).SetBodyJsonMarshal(payload).Post(pushURL)

				return response.StatusCode
			}).Should(Equal(http.StatusOK))
		}

		push("web", "GET /index 200", base)
		push("db", "SELECT slow query", base+1000)

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(BeNumerically(">=", 2))

		var queryResponse loge.QueryResponse

		Eventually(func() int {
			body := map[string]any{
				"matchers": []map[string]string{{"name": "app", "value": "web", "type": "="}},
			}

			response, err := httpClient.R().
				SetBodyJsonMarshal(body).
				SetSuccessResult(&queryResponse).
				Post(queryURL)
			if err != nil || response.StatusCode != http.StatusOK {
				return -1
			}

			return len(queryResponse.Data)
		}, "5s").Should(Equal(1))

		Expect(queryResponse.Status).To(Equal("success"))
		Expect(queryResponse.Data[0].Labels).To(HaveKeyWithValue("app", "web"))
		Expect(queryResponse.Data[0].Line).To(Equal("GET /index 200"))
	})

	It("queries pushed logs by a raw LogQL selector", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		httpClient := req.C()
		pushURL := fmt.Sprintf("http://localhost:%d/api/v1/push", port)
		queryURL := fmt.Sprintf("http://localhost:%d/api/v1/query", port)

		base := int64(1_700_000_000_000_000_000)
		push := func(app, line string, ts int64) {
			payload := loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": app},
						Values: loge.Values{loge.Value{strconv.FormatInt(ts, 10), line}},
					},
				},
			}

			Eventually(func() int {
				response, _ := httpClient.R().SetRetryCount(3).SetBodyJsonMarshal(payload).Post(pushURL)

				return response.StatusCode
			}).Should(Equal(http.StatusOK))
		}

		push("web", "GET /index 200", base)
		push("db", "SELECT slow query", base+1000)

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(BeNumerically(">=", 2))

		var queryResponse loge.QueryResponse

		// A single LogQL selector (as the web UI sends) is parsed server-side
		// into matchers + line filter, so it matches the same web row.
		Eventually(func() int {
			response, err := httpClient.R().
				SetBodyJsonMarshal(map[string]any{"query": `{app="web"} |= "index"`}).
				SetSuccessResult(&queryResponse).
				Post(queryURL)
			if err != nil || response.StatusCode != http.StatusOK {
				return -1
			}

			return len(queryResponse.Data)
		}, "5s").Should(Equal(1))

		Expect(queryResponse.Data[0].Labels).To(HaveKeyWithValue("app", "web"))
		Expect(queryResponse.Data[0].Line).To(Equal("GET /index 200"))

		// A malformed selector is a 400, not a silently-empty result.
		badResp, err := httpClient.R().
			SetBodyJsonMarshal(map[string]any{"query": `{app=}`}).
			Post(queryURL)
		Expect(err).NotTo(HaveOccurred())
		Expect(badResp.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("gates read endpoints behind the api key when one is set", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
			"--api-key", "secret",
		)

		httpClient := req.C()
		base := fmt.Sprintf("http://localhost:%d", port)

		// The server is up once the auth probe answers (401 without a token).
		Eventually(func() int {
			response, _ := httpClient.R().SetRetryCount(3).Get(base + "/api/v1/auth")

			return response.StatusCode
		}).Should(Equal(http.StatusUnauthorized))

		auth := func(r *req.Request) *req.Request { return r.SetHeader("Authorization", "Bearer secret") }

		// Reads require the token: 401 without, 200 with.
		for _, path := range []string{"/api/v1/auth", "/api/v1/labels", "/api/v1/stats"} {
			resp, err := httpClient.R().Get(base + path)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized), path+" without key")

			resp, err = auth(httpClient.R()).Get(base + path)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK), path+" with key")
		}

		// The auth probe reports that a key is required.
		var authResp struct {
			Required bool `json:"required"`
		}
		_, err = auth(httpClient.R()).SetSuccessResult(&authResp).Get(base + "/api/v1/auth")
		Expect(err).NotTo(HaveOccurred())
		Expect(authResp.Required).To(BeTrue())

		// Query is gated too.
		resp, err := httpClient.R().SetBodyString(`{}`).Post(base + "/api/v1/query")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

		resp, err = auth(httpClient.R()).SetBodyString(`{}`).Post(base + "/api/v1/query")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		// Ingest stays open so log shippers don't need the key.
		resp, err = httpClient.R().SetBodyJsonMarshal(generatePayload()).Post(base + "/api/v1/push")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).NotTo(Equal(http.StatusUnauthorized))

		// The web UI's static assets stay open so the login page can load.
		resp, err = httpClient.R().Get(base + "/")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("serves the embedded web UI", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		StartCLI(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)

		httpClient := req.C()
		base := fmt.Sprintf("http://localhost:%d", port)

		var index *req.Response
		Eventually(func() int {
			resp, err := httpClient.R().Get(base + "/")
			if err != nil {
				return -1
			}
			index = resp

			return resp.StatusCode
		}).Should(Equal(http.StatusOK))

		Expect(index.GetContentType()).To(ContainSubstring("text/html"))
		body := index.String()
		Expect(body).To(ContainSubstring("<!doctype html"))
		Expect(body).To(ContainSubstring(`id="app"`))
		Expect(body).To(ContainSubstring("/assets/app.js"))

		// Bundled assets are served too.
		js, err := httpClient.R().Get(base + "/assets/app.js")
		Expect(err).NotTo(HaveOccurred())
		Expect(js.StatusCode).To(Equal(http.StatusOK))
		Expect(js.GetContentType()).To(ContainSubstring("javascript"))

		css, err := httpClient.R().Get(base + "/assets/app.css")
		Expect(err).NotTo(HaveOccurred())
		Expect(css.StatusCode).To(Equal(http.StatusOK))
		Expect(css.GetContentType()).To(ContainSubstring("css"))
	})
})

// nolint: gosec
func generatePayload() *loge.Payload {
	payload := &loge.Payload{}
	fake := faker.New()

	for range rand.Intn(10) + 1 {
		entry := loge.Entry{
			Stream: loge.Stream{},
		}

		for range rand.Intn(10) + 1 {
			entry.Stream[fake.Person().Name()] = fake.Lorem().Text(100)
		}

		for range rand.Intn(10) + 1 {
			entry.Values = append(entry.Values, loge.Value{
				strconv.FormatInt(time.Now().UnixNano(), 10),
				fake.Lorem().Sentence(10),
			})
		}

		payload.Streams = append(payload.Streams, entry)
	}

	return payload
}
