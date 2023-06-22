package local

import (
	"errors"
	"io"
	"os"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/must/v2"
)

type tailer struct {
	f      *os.File
	w      *fsnotify.Watcher
	path   string
	closed chan struct{}
}

func tail(p string) (tailer, error) {
	f, err := os.Open(p)
	if err != nil {
		return tailer{}, err
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		f.Close()
		return tailer{}, err
	}
	if err := w.Add(path.Dir(p)); err != nil {
		w.Close()
		f.Close()
		return tailer{}, err
	}

	return tailer{f: f, w: w, path: p, closed: make(chan struct{})}, nil
}

func (t tailer) Close() error {
	close(t.closed)
	must.OK(t.w.Close())
	return t.f.Close()
}

func (t tailer) Read(buf []byte) (int, error) {
	if _, err := os.Lstat(t.path); err != nil { // file does not exist
		return 0, api.ErrContinuityBroken
	}

	for {
		n, err := t.f.Read(buf)
		if !errors.Is(err, io.EOF) { // no error or an error other than EOF
			return n, err
		}
		if n > 0 {
			return n, nil
		}
		if err := t.waitForMore(t.w); err != nil {
			return n, err
		}
	}
}

func (t tailer) Size() (int64, error) {
	stat, err := t.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (t tailer) waitForMore(w *fsnotify.Watcher) error {
	for {
		select {
		case <-t.closed:
			return os.ErrClosed
		case event := <-w.Events:
			if event.Name != t.path {
				continue
			}
			switch {
			case event.Op&fsnotify.Remove != 0:
				return api.ErrContinuityBroken
			case event.Op&fsnotify.Write != 0:
				return nil
			}
		case err := <-w.Errors:
			return err
		}
	}
}
