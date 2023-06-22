package local

import (
	"context"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/parallel"
)

func (c client) LastOffset(ctx context.Context, topic string) (int64, error) {
	var offset int64
	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		messages := make(chan *api.IncomingMessage)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return c.Read(ctx, topic, 0, messages)
		})
		spawn("counter", parallel.Exit, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg := <-messages:
					if msg == nil {
						return nil
					}
					offset++
				}
			}
		})
		return nil
	})
	return offset, err
}
