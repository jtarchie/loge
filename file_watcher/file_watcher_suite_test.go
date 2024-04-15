package filewatcher_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	filewatcher "github.com/jtarchie/loge/file_watcher"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFileWatcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FileWatcher Suite")
}

var _ = Describe("FileWatcher", func() {
	It("errors when path does not exist", func() {
		_, err := filewatcher.New("cannot-exist-123", nil)
		Expect(err).To(HaveOccurred())
	})

	When("directory exists", func() {
		var outputPath string

		BeforeEach(func() {
			var err error

			outputPath, err = os.MkdirTemp("", "")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns an error when iterating errors", func() {
			for range 10 {
				_, err := os.Create(filepath.Join(outputPath, fmt.Sprintf("file-%d.txt", time.Now().UnixNano())))
				Expect(err).NotTo(HaveOccurred())
			}

			watcher, err := filewatcher.New(outputPath, regexp.MustCompile(`file-\d+\.txt$`))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Close()

			err = watcher.Iterate(func(filename string) error {
				return errors.New("some error")
			})
			Expect(err).To(HaveOccurred())
		})

		It("does nothing if there are no files", func() {
			watcher, err := filewatcher.New(outputPath, nil)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Close()

			var files []string

			err = watcher.Iterate(func(filename string) error {
				files = append(files, filename)

				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(files).To(BeEmpty())
		})

		It("iterates over files already existing", func() {
			for range 10 {
				_, err := os.Create(filepath.Join(outputPath, fmt.Sprintf("file-%d.txt", time.Now().UnixNano())))
				Expect(err).NotTo(HaveOccurred())
			}

			watcher, err := filewatcher.New(outputPath, regexp.MustCompile(`file-\d+\.txt$`))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Close()

			var files []string

			err = watcher.Iterate(func(filename string) error {
				files = append(files, filename)

				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(files).Should(HaveLen(10))
		})

		It("iterates (eventually) over files created", func() {
			for range 10 {
				_, err := os.Create(filepath.Join(outputPath, fmt.Sprintf("file-%d.txt", time.Now().UnixNano())))
				Expect(err).NotTo(HaveOccurred())
			}

			watcher, err := filewatcher.New(outputPath, regexp.MustCompile(`file-\d+\.txt$`))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Close()

			for range 10 {
				_, err := os.Create(filepath.Join(outputPath, fmt.Sprintf("file-%d.txt", time.Now().UnixNano())))
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() []string {
				var files []string

				_ = watcher.Iterate(func(filename string) error {
					files = append(files, filename)

					return nil
				})

				return files
			}).Should(HaveLen(20), "5s")
		})
	})
})
