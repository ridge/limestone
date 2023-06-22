package local

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/names"
	"github.com/ridge/limestone/kafka/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
)

func (c client) Read(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) error {
	must.OK(names.ValidateTopicName(topic))
	path := filepath.Join(c.dir, topic)
	f, err := tail(path)
	if os.IsNotExist(err) {
		if offset == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- nil:
			}
		}
		if err := waitToAppear(ctx, c.dir, path); err != nil {
			return err
		}
		f, err = tail(path)
	}
	must.OK(err)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			r := bufio.NewReader(f)
			var pos int64
			bytesRead := 0
			for {
				if pos > 0 && pos >= offset {
					// f.Size might return "use of closed file" when ctx is closing
					size, err := f.Size()
					if ctx.Err() != nil {
						return ctx.Err()
					}
					must.OK(err)

					if int64(bytesRead) == size {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case dest <- nil:
						}
					}
				}

				msg, n, err := readMessage(topic, r, pos)
				bytesRead += n
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, api.ErrContinuityBroken) {
					return api.ErrContinuityBroken
				}
				must.OK(err)

				if msg.Offset >= offset {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case dest <- &msg:
					}
				}
				pos = msg.Offset + 1
			}
		})
		spawn("closer", parallel.Exit, func(ctx context.Context) error {
			<-ctx.Done()
			return f.Close()
		})
		return nil
	})
}

func readMessage(topic string, r *bufio.Reader, defaultOffset int64) (msg api.IncomingMessage, n int, err error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return api.IncomingMessage{}, 0, err
	}
	var h wire.Header
	must.OK(json.Unmarshal(b, &h))

	body := make([]byte, h.Len+1)
	_, err = io.ReadFull(r, body)
	if err != nil {
		return api.IncomingMessage{}, 0, err
	}
	if body[h.Len] != '\n' {
		panic("invalid topic file format")
	}

	offset := defaultOffset
	if h.Offset != nil {
		offset = *h.Offset
		if offset < defaultOffset {
			panic("non-monotonic offsets")
		}
	}

	return api.IncomingMessage{
		Message: api.Message{
			Topic:   topic,
			Key:     h.Key,
			Headers: h.Headers,
			Value:   body[:h.Len],
		},
		Time:   h.TS,
		Offset: offset,
	}, len(b) + len(body), nil
}

func waitToAppear(ctx context.Context, dir, path string) error {
	w := must.OK1(fsnotify.NewWatcher())
	defer must.Do(w.Close)
	must.OK(w.Add(dir))

	for {
		if _, err := os.Lstat(path); err == nil { // file exists
			return nil
		}
		if err := waitForPath(ctx, w, path); err != nil {
			return err
		}
	}
}

func waitForPath(ctx context.Context, w *fsnotify.Watcher, path string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-w.Events:
			if event.Name == path {
				return nil
			}
		case err := <-w.Errors:
			must.OK(err)
		}
	}
}
