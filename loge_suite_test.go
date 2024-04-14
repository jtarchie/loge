package loge_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/imroc/req/v3"
	"github.com/jaswdr/faker/v2"
	"github.com/jtarchie/loge"
	"github.com/jtarchie/sqlitezstd"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/phayes/freeport"
	"github.com/samber/lo"
	"github.com/tinylib/msgp/msgp"
)

func TestLoge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Loge Suite")
}

type cli struct{}

func (c *cli) Kill() {}

var _ = Describe("Running the application", func() {
	cli := func(args ...string) *cli {
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

		runtime.Gosched()

		return &cli{}
	}

	BeforeEach(func() {
		err := sqlitezstd.Init()
		Expect(err).NotTo(HaveOccurred())
	})

	It("accepts a JSON payload", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		session := cli(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)
		defer session.Kill()

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
		}).Should(BeNumerically(">=", 1))

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
	})

	It("accepts a MsgPack payload", func() {
		outputPath, err := os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		session := cli(
			"--port", strconv.Itoa(port),
			"--buckets", "1",
			"--payload-size", "1",
			"--output-path", outputPath,
		)
		defer session.Kill()

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
		}).Should(BeNumerically(">=", 1))

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

		value := lo.Values(payload.Streams[0].Stream)[0]
		err = dbClient.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM search WHERE search MATCH '%s'", value)).Scan(&count)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 1))
	})
})

//nolint: gosec
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
