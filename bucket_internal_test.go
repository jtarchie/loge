package loge

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCompressDoesNotPublishOnError ensures a failed compression does not
// publish a (possibly corrupt) .zst and does not leave a stray .partial file.
func TestCompressDoesNotPublishOnError(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "missing.sqlite")

	// The source file does not exist, so compression fails after the partial
	// output file has already been created.
	if err := compress(source); err == nil {
		t.Fatal("expected an error compressing a missing source file")
	}

	entries, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatalf("glob failed: %s", err)
	}

	if len(entries) != 0 {
		t.Fatalf("expected no leftover files after failed compression, found: %v", entries)
	}
}

// TestCompressPublishesAndRemovesSourceOnSuccess ensures a successful
// compression publishes the .zst and removes the source file.
func TestCompressPublishesAndRemovesSourceOnSuccess(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "data.sqlite")

	if err := os.WriteFile(source, []byte("hello loge"), 0o600); err != nil {
		t.Fatalf("could not write source: %s", err)
	}

	if err := compress(source); err != nil {
		t.Fatalf("compress failed: %s", err)
	}

	if _, err := os.Stat(source + ".zst"); err != nil {
		t.Fatalf("expected published .zst, got: %s", err)
	}

	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("expected source to be removed, stat err: %v", err)
	}

	if _, err := os.Stat(source + ".zst.partial"); !os.IsNotExist(err) {
		t.Fatalf("expected no leftover .partial, stat err: %v", err)
	}
}
