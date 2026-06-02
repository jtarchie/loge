package filewatcher

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"

	"github.com/fsnotify/fsnotify"
)

type FileWatcher struct {
	compiled *regexp.Regexp
	files    *sync.Map
	watcher  *fsnotify.Watcher
}

func New(path string, compiled *regexp.Regexp) (*FileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("could not start watcher: %w", err)
	}

	err = watcher.Add(path)
	if err != nil {
		return nil, fmt.Errorf("could not add path to watcher (%q): %w", path, err)
	}

	fileWatcher := &FileWatcher{
		compiled: compiled,
		files:    &sync.Map{},
		watcher:  watcher,
	}

	matches, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return nil, fmt.Errorf("could not glob current path: %w", err)
	}

	for _, match := range matches {
		fileWatcher.add(match)
	}

	go fileWatcher.init()
	runtime.Gosched() // give go routine sometime

	return fileWatcher, nil
}

func (f *FileWatcher) add(filename string) {
	abs, err := filepath.Abs(filename)
	if err != nil {
		slog.Warn("could not resolve absolute path for watched file",
			slog.String("filename", filename),
			slog.String("error", err.Error()),
		)

		return
	}

	if f.compiled == nil || !f.compiled.MatchString(abs) {
		return
	}

	// Only track files that actually exist; a transient name (e.g. the source
	// side of a rename) must not be stored.
	if _, err := os.Stat(abs); err != nil {
		return
	}

	f.files.Store(abs, struct{}{})
}

func (f *FileWatcher) remove(filename string) {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return
	}

	f.files.Delete(abs)
}

func (f *FileWatcher) init() {
	for event := range f.watcher.Events {
		switch {
		case event.Has(fsnotify.Create):
			f.add(event.Name)
		case event.Has(fsnotify.Remove), event.Has(fsnotify.Rename):
			// A removed or renamed-away file must leave the set so queries do
			// not keep trying to open a path that no longer exists.
			f.remove(event.Name)
		}
	}
}

func (f *FileWatcher) Iterate(fun func(string) error) error {
	var errs []error

	f.files.Range(func(file, _ interface{}) bool {
		if err := fun(file.(string)); err != nil {
			// Collect and keep going: one bad file must not hide every other
			// file's results.
			errs = append(errs, fmt.Errorf("watcher failed execution (%q): %w", file.(string), err))
		}

		return true
	})

	return errors.Join(errs...)
}

func (f *FileWatcher) Close() error {
	err := f.watcher.Close()
	if err != nil {
		return fmt.Errorf("could not close watcher: %w", err)
	}

	return nil
}
