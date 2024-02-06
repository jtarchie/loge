package loge_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/imroc/req/v3"
	"github.com/jtarchie/loge"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/phayes/freeport"
	"github.com/pioz/faker"
	"github.com/tinylib/msgp/msgp"
)

func TestLoge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Loge Suite")
}

var _ = Describe("Running the application", func() {
	var path string

	cli := func(args ...string) *gexec.Session {
		command := exec.Command(path, args...)

		session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred())

		return session
	}

	BeforeEach(func() {
		var err error

		path, err = gexec.Build("github.com/jtarchie/loge/loge", "--tags", "fts5")
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

		client := req.C()

		Consistently(func() int {
			response, _ := client.R().
				SetRetryCount(3).
				SetBodyJsonMarshal(payload).
				Put(fmt.Sprintf("http://localhost:%d/api/streams", port))

			//nolint: wrapcheck
			return response.StatusCode
		}).Should(Equal(http.StatusOK))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite"))
			return len(matches)
		}).Should(BeNumerically(">=", 1))

		matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite"))
		Expect(err).NotTo(HaveOccurred())

		sqliteFilename := matches[0]
		db, err := sql.Open("sqlite3", sqliteFilename)
		Expect(err).NotTo(HaveOccurred())

		var count int

		err = db.QueryRow("SELECT COUNT(*) FROM labels").Scan(&count)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 1))

		err = db.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
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

		client := req.C()

		Consistently(func() int {
			response, _ := client.R().
				SetRetryCount(3).
				SetContentType("application/msgpack").
				SetBodyBytes(contents.Bytes()).
				Put(fmt.Sprintf("http://localhost:%d/api/streams", port))

			//nolint: wrapcheck
			return response.StatusCode
		}).Should(Equal(http.StatusOK))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite"))
			return len(matches)
		}).Should(BeNumerically(">=", 1))

		matches, err := filepath.Glob(filepath.Join(outputPath, "*.sqlite"))
		Expect(err).NotTo(HaveOccurred())

		sqliteFilename := matches[0]
		db, err := sql.Open("sqlite3", sqliteFilename)
		Expect(err).NotTo(HaveOccurred())

		var count int

		err = db.QueryRow("SELECT COUNT(*) FROM labels").Scan(&count)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 1))

		err = db.QueryRow("SELECT COUNT(*) FROM streams").Scan(&count)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 1))
	})
})

func generatePayload() *loge.Payload {
	payload := &loge.Payload{}

	for i := 0; i < rand.Intn(10)+1; i++ {
		entry := loge.Entry{
			Stream: loge.Stream{},
		}

		for i := 0; i < rand.Intn(10)+1; i++ {
			entry.Stream[faker.Username()] = faker.Letters()
		}

		for i := 0; i < rand.Intn(10)+1; i++ {
			entry.Values = append(entry.Values, loge.Value{
				fmt.Sprint(time.Now().UnixNano()),
				faker.Sentence(),
			})
		}

		payload.Streams = append(payload.Streams, entry)
	}

	return payload
}
