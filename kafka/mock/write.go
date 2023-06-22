package mock

import (
	"context"
	"fmt"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/names"
	"github.com/ridge/must/v2"
	"time"
)

// Write implements the Write method of the kafka.Client interface (see
// documentation) by writing to the simulated database.
func (k *kafka) Write(ctx context.Context, topicName string, messages []api.Message) error {
	if len(messages) == 0 {
		return nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	ts := time.Now()
	batch := make([]api.IncomingMessage, 0, len(messages))
	for _, m := range messages {
		batch = append(batch, api.IncomingMessage{
			Message: m,
			Time:    ts,
		})
	}
	return k.writeImpl(topicName, batch)
}

// WriteBackdated implements the WriteBackdated method of the
// kafka.ClientBackdate interface (see documentation) by writing to the
// simulated database.
func (k *kafka) WriteBackdated(ctx context.Context, topicName string, messages []api.IncomingMessage) error {
	if len(messages) == 0 {
		return nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	return k.writeImpl(topicName, messages)
}

func (k *kafka) writeImpl(topicName string, messages []api.IncomingMessage) error {
	must.OK(names.ValidateTopicName(topicName))

	t := k.topics[topicName]
	if t == nil {
		t = &topic{more: make(chan struct{})}
		k.topics[topicName] = t
		close(k.more)
		k.more = make(chan struct{})
	}

	var last time.Time
	if len(t.data) > 0 {
		last = t.data[len(t.data)-1].ts
	}

	for _, m := range messages {
		if m.Topic != topicName {
			panic(fmt.Errorf("topic mismatch on write: %s (expected %s)", m.Topic, topicName))
		}
		if last.After(m.Time) {
			panic(fmt.Errorf("non-monotonic timestamps in topic %s: %v after %v", topicName, m.Time, last))
		}
		last = m.Time
		t.data = append(t.data, message{
			ts:      m.Time,
			key:     m.Key,
			headers: m.Headers,
			value:   m.Value,
		})
	}

	close(t.more)
	t.more = make(chan struct{})

	return nil
}
