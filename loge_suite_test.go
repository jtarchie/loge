package loge_test

import (
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"testing"

	"github.com/imroc/req/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/phayes/freeport"
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
	})
})