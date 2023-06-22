package kafkago

import (
	"context"
	"fmt"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"time"
)

const writerTimeout = time.Minute

var writerRetry = retry.FixedConfig{RetryAfter: time.Second}

func (c *client) Write(ctx context.Context, topic string, messages []api.Message) error {
	if len(messages) == 0 {
		return nil
	}

	ctx = tlog.With(ctx, zap.String("topic", topic))
	batch := make([]kafka.Message, 0, len(messages))
	for _, m := range messages {
		if m.Topic != topic {
			panic(fmt.Errorf("topic mismatch on write: %s (expected %s)", m.Topic, topic))
		}
		headers := make([]kafka.Header, 0, len(m.Headers))
		for k, v := range m.Headers {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}
		batch = append(batch, kafka.Message{
			Key:     []byte(m.Key),
			Value:   m.Value,
			Headers: headers,
		})
	}

	return retry.Do(ctx, writerRetry, func() error {
		if _, err := c.writeBatch(ctx, topic, batch); err != nil {
			if shouldRetry(err) {
				return retry.Retriable(fmt.Errorf("failed to write Kafka messages: %w", err))
			}
			return err
		}
		return nil
	})
}

func (c *client) writeBatch(ctx context.Context, topic string, batch []kafka.Message) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	conn, err := c.getKafkaConn(ctx, topic)
	if err != nil {
		return 0, err
	}

	deadline, _ := ctx.Deadline()       // ok is definitely true because of context.WithTimeout
	must.OK(conn.SetDeadline(deadline)) // kafka-go always returns nil from conn.SetDeadline

	nbytes, err := conn.WriteMessages(batch...)
	if err != nil {
		c.invalidateKafkaConn(topic, conn)
		_ = conn.Close()
	}
	return nbytes, err
}
