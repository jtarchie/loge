package filewatcher

import (
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"

	"github.com/fsnotify/fsnotify"
)

type FileWatcher struct {
	compiled *regexp.Regexp
	files    map[string]struct{}
	mutex    *sync.RWMutex
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
		files:    map[string]struct{}{},
		mutex:    &sync.RWMutex{},
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
	f.mutex.Lock()
	defer f.mutex.Unlock()

	filename, _ = filepath.Abs(filename)
	if f.compiled.MatchString(filename) {
		f.files[filename] = struct{}{}
	}
}

func (f *FileWatcher) init() {
	for event := range f.watcher.Events {
		if event.Has(fsnotify.Create) {
			f.add(event.Name)
		}
	}
}

func (f *FileWatcher) Iterate(fun func(string) error) error {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	for file := range f.files {
		err := fun(file)
		if err != nil {
			return fmt.Errorf("watcher failed execution: %w", err)
		}
	}

	return nil
}

func (f *FileWatcher) Close() error {
	err := f.watcher.Close()
	if err != nil {
		return fmt.Errorf("could not close watcher: %w", err)
	}

	return nil
}
