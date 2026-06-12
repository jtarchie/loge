package loge_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-resty/resty/v2"
	"github.com/jtarchie/loge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/phayes/freeport"
)

var _ = Describe("loge search", func() {
	It("queries a running server with a LogQL selector", func() {
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

		httpClient := resty.New().SetRetryCount(3)
		pushURL := fmt.Sprintf("http://localhost:%d/api/v1/push", port)
		addr := fmt.Sprintf("http://localhost:%d", port)

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
				response, _ := httpClient.R().SetBody(payload).Post(pushURL)

				return response.StatusCode()
			}).Should(Equal(http.StatusOK))
		}

		push("web", "GET /index 200", base)
		push("db", "SELECT slow query", base+1000)

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(BeNumerically(">=", 2))

		By("filtering by a stream-label matcher")
		labelOut := &bytes.Buffer{}
		labelCmd := &loge.SearchCmd{Selector: `{app="web"}`, Addr: addr, Limit: 100, Output: "text", Out: labelOut}

		Eventually(func() string {
			labelOut.Reset()
			Expect(labelCmd.Run()).To(Succeed())

			return labelOut.String()
		}, "5s").Should(ContainSubstring("GET /index 200"))
		Expect(labelOut.String()).NotTo(ContainSubstring("SELECT slow query"))

		By("filtering by a keyword (line filter) with JSON output")
		keywordOut := &bytes.Buffer{}
		keywordCmd := &loge.SearchCmd{Selector: `{app="web"} |= "index"`, Addr: addr, Limit: 100, Output: "json", Out: keywordOut}

		Eventually(func() string {
			keywordOut.Reset()
			Expect(keywordCmd.Run()).To(Succeed())

			return keywordOut.String()
		}, "5s").Should(ContainSubstring("GET /index 200"))
	})

	It("returns a clear error when the server is unreachable", func() {
		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		cmd := &loge.SearchCmd{
			Selector: `{app="web"}`,
			Addr:     fmt.Sprintf("http://localhost:%d", port),
			Limit:    100,
			Output:   "text",
			Out:      &bytes.Buffer{},
		}

		err = cmd.Run()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("could not reach loge server"))
	})
})
