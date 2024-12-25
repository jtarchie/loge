package loge_test

import (
	"bytes"
	"encoding/json"
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

		response, err := httpClient.R().
			SetRetryCount(3).
			Get(fmt.Sprintf("http://localhost:%d/api/v1/labels", port))
		Expect(err).NotTo(HaveOccurred())

		err = json.NewDecoder(response.Body).Decode(&labelResponse)
		Expect(err).NotTo(HaveOccurred())
		Expect(labelResponse.Status).To(Equal("success"))
		Expect(labelResponse.Data).To(ConsistOf(knownLabels))
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

		response, err := httpClient.R().
			SetRetryCount(3).
			SetHeader("Accept", "application/msgpack").
			Get(fmt.Sprintf("http://localhost:%d/api/v1/labels", port))
		Expect(err).NotTo(HaveOccurred())

		err = msgp.Decode(response.Body, &labelResponse)
		Expect(err).NotTo(HaveOccurred())
		Expect(labelResponse.Status).To(Equal("success"))
		Expect(labelResponse.Data).To(ConsistOf(knownLabels))
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
