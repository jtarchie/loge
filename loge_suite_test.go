package loge_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/imroc/req/v3"
	"github.com/jtarchie/loge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/phayes/freeport"
	"github.com/pioz/faker"
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

	It("accept a payload", func() {
		port, err := freeport.GetFreePort()
		Expect(err).NotTo(HaveOccurred())

		session := cli("--port", strconv.Itoa(port))
		defer session.Kill()

		payload := generatePayload()

		client := req.C()

		Eventually(func() int {
			response, _ := client.R().
				SetRetryCount(3).
				SetBodyJsonMarshal(payload).
				Put(fmt.Sprintf("http://localhost:%d/api/streams", port))

			//nolint: wrapcheck
			return response.StatusCode
		}).Should(Equal(http.StatusOK))

		// saves the file to the database

		// can search the database via endpoint
	})
})

func generatePayload() *loge.Payload {
	payload := &loge.Payload{}

	for i := 0; i < rand.Intn(10)+1; i++ {
	payload := &loge.Payload{}
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