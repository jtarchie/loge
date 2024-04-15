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
	files    []string
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
		files:    []string{},
		mutex:    &sync.RWMutex{},
		watcher:  watcher,
	}

	matches, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return nil, fmt.Errorf("could not glob current path: %w", err)
	}

	for _, match := range matches {
		if compiled.MatchString(match) {
			fileWatcher.files = append(fileWatcher.files, match)
		}
	}

	go fileWatcher.init()
	runtime.Gosched() // give go routine sometime

	return fileWatcher, nil
}

func (f *FileWatcher) init() {
	for event := range f.watcher.Events {
		if event.Has(fsnotify.Create) && f.compiled.MatchString(event.Name) {
			f.mutex.Lock()
			f.files = append(f.files, event.Name)
			f.mutex.Unlock()
		}
	}
}

func (f *FileWatcher) Iterate(fun func(string) error) error {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	for _, file := range f.files {
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
