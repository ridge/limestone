package kafkago

import (
	"context"
	"fmt"

	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
	"time"
)

// This timeout includes the time it takes to establish a connection to the broker.
// Once #16815 is fixed, we can reduce this timeout back to 10 seconds.
const lastOffsetTimeout = time.Minute

func (c *client) LastOffset(ctx context.Context, topic string) (offset int64, err error) {
	ctx = tlog.With(ctx, zap.String("topic", topic))
	err = retry.Do(ctx, readerRetry, func() error {
		var err error
		offset, err = c.lastOffset(ctx, topic)
		if err != nil {
			if shouldRetry(err) {
				return retry.Retriable(fmt.Errorf("failed to retrieve last offset: %w", err))
			}
			return err
		}
		return nil
	})
	return offset, err
}

func (c *client) lastOffset(ctx context.Context, topic string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, lastOffsetTimeout)
	defer cancel()

	conn, err := c.getKafkaConn(ctx, topic)
	if err != nil {
		return 0, err
	}

	deadline, _ := ctx.Deadline()       // ok is definitely true because of context.WithTimeout
	must.OK(conn.SetDeadline(deadline)) // kafka-go always returns nil from conn.SetDeadline

	return conn.ReadLastOffset()
}
