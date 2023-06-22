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

const (
	readerMaxBytes = 1e7
	readerMaxWait  = time.Hour
)

var readerRetry = retry.FixedConfig{RetryAfter: time.Second}

func (c *client) Read(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) error {
	ctx = tlog.With(ctx, zap.String("topic", topic))
	logger := tlog.Get(ctx)
	logger.Info("Reading from Kafka topic", zap.Int64("offset", offset))

	kr := c.kafkaAPI.NewReader(kafka.ReaderConfig{
		Brokers:  c.brokers,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: readerMaxBytes,
		MaxWait:  readerMaxWait,
	})
	defer must.Do(kr.Close)

	must.OK(kr.SetOffset(offset))

	first := true
	var offsetFrom, offsetTo int64
	count := 0
	for ctx.Err() == nil {
		message, more, err := readMessage(ctx, first, kr)
		if err != nil {
			return err
		}

		if message != nil {
			if count == 0 {
				offsetFrom = message.Offset
			}
			offsetTo = message.Offset
			count++

			incoming := &api.IncomingMessage{
				Message: api.Message{
					Topic:   topic,
					Key:     string(message.Key),
					Headers: map[string]string{},
					Value:   message.Value,
				},
				Time:   message.Time,
				Offset: message.Offset,
			}
			for _, h := range message.Headers {
				incoming.Headers[h.Key] = string(h.Value)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- incoming:
			}
		}

		if !more {
			if first && count == 0 {
				logger.Debug("Reached hot end without obtaining any Kafka messages", zap.Int64("offset", offset))
			} else {
				logger.Debug("Obtained a batch of Kafka messages and reached hot end",
					zap.Int("n", count), zap.Int64("first", offsetFrom), zap.Int64("last", offsetTo))
			}
			count = 0

			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- nil:
			}
		}

		first = false
	}

	return ctx.Err()
}

// readMessage reads one message from the topic.
// Returns nil at the hot end.
// Retries until success.
func readMessage(ctx context.Context, needLag bool, kr kafkaReader) (msg *kafka.Message, more bool, err error) {
	err = retry.Do(ctx, readerRetry, func() error {
		if needLag {
			lag, err := kr.ReadLag(ctx)
			if err != nil {
				if shouldRetry(err) {
					return retry.Retriable(fmt.Errorf("failed to retrieve Kafka topic lag: %w", err))
				}
				return err
			}
			switch {
			case lag < 0:
				return api.ErrContinuityBroken
			case lag == 0:
				return nil
			}
		}

		m, err := kr.FetchMessage(ctx)
		if err != nil {
			if shouldRetry(err) {
				needLag = true
				return retry.Retriable(fmt.Errorf("failed to read from Kafka topic: %w", err))
			}
			return err
		}
		msg = &m
		more = kr.Lag() > 0
		return nil
	})
	return msg, more, err
}
