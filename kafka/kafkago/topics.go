package kafkago

import (
	"context"
	"fmt"

	"github.com/ridge/limestone/retry"
	"github.com/ridge/parallel"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func (c *client) Topics(ctx context.Context) (res []string, err error) {
	err = retry.Do(ctx, readerRetry, func() error {
		var err error
		res, err = c.listTopics(ctx)
		if err != nil {
			if shouldRetry(err) {
				return retry.Retriable(fmt.Errorf("failed to retrieve topic list: %w", err))
			}
			return err
		}
		return nil
	})
	return res, err
}

func (c *client) listTopics(ctx context.Context) (res []string, err error) {
	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		conn, err := c.connectMaster(ctx)
		if err != nil {
			return err
		}
		spawn("partitions", parallel.Exit, func(ctx context.Context) error {
			partitions, err := conn.ReadPartitions()
			if err != nil {
				return err
			}
			topics := map[string]bool{}
			for _, p := range partitions {
				topics[p.Topic] = true
			}
			res = maps.Keys(topics)
			slices.Sort(res)
			return nil
		})
		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			conn.Close()
			return ctx.Err()
		})
		return nil
	})
	return res, err
}
