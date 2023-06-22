package mock

import (
	"context"

	"github.com/ridge/limestone/kafka/api"
)

// Read implements the Read method of the kafka.Client interface (see
// documentation) by reading from the simulated database.
func (k *kafka) Read(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) error {
	eof := false

	for {
		messages, more := k.read(topic, int(offset))

		for _, m := range messages {
			im := &api.IncomingMessage{
				Message: api.Message{
					Topic:   topic,
					Key:     m.key,
					Headers: m.headers,
					Value:   m.value,
				},
				Time:   m.ts,
				Offset: offset,
			}
			offset++
			eof = false
			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- im:
			}
		}

		if !eof {
			eof = true
			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- nil:
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-more:
		}
	}
}

func (k *kafka) read(topic string, from int) ([]message, <-chan struct{}) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	t := k.topics[topic]
	if t == nil {
		return nil, k.more // lets the consumer wait for new topics to be created
	}

	if len(t.data) < from {
		return nil, t.more
	}

	return t.data[from:], t.more
}
