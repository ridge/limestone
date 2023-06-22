package local

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/names"
	"github.com/ridge/limestone/kafka/wire"
	"github.com/ridge/must/v2"
	"time"
)

func (c client) Write(ctx context.Context, topic string, messages []api.Message) error {
	ts := time.Now()
	batch := make([]api.IncomingMessage, 0, len(messages))
	for _, m := range messages {
		batch = append(batch, api.IncomingMessage{
			Message: m,
			Time:    ts,
		})
	}
	return c.WriteBackdated(ctx, topic, batch)
}

func (c client) WriteBackdated(ctx context.Context, topic string, messages []api.IncomingMessage) error {
	must.OK(names.ValidateTopicName(topic))
	if len(messages) == 0 {
		return nil
	}

	f, err := os.OpenFile(filepath.Join(c.dir, topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to append topic %s: %w", topic, err)
	}
	defer must.Do(f.Close)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("failed to append topic %s: %w", topic, err)
	}
	defer must.Do(func() error {
		return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	})

	// prepare a single write to minimize the likelihood of leaving the file corrupted
	var buf bytes.Buffer

	for _, msg := range messages {
		if msg.Topic != topic {
			panic(fmt.Errorf("topic mismatch on write: %s (expected %s)", msg.Topic, topic))
		}
		must.OK1(buf.Write(must.OK1(json.Marshal(wire.Header{
			TS:      msg.Time,
			Key:     msg.Key,
			Headers: msg.Headers,
			Len:     len(msg.Value),
		}))))
		must.OK(buf.WriteByte('\n'))
		must.OK1(buf.Write(msg.Value))
		must.OK(buf.WriteByte('\n'))
	}

	if _, err := f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to append topic %s: %w", topic, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to append topic %s: %w", topic, err)
	}

	return nil
}
