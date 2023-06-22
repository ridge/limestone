// Package isempty contains the TopicIsEmpty function.
//
// Outside lib/kafka, don't import this package directly. Instead, import
// lib/kafka which reexports TopicIsEmpty.
package isempty

import (
	"context"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/parallel"
)

// TopicIsEmpty finds out whether a Kafka topic is empty
func TopicIsEmpty(ctx context.Context, kc api.Client, topic string) (bool, error) {
	var empty bool
	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		ch := make(chan *api.IncomingMessage)
		spawn("read", parallel.Fail, func(ctx context.Context) error {
			return kc.Read(ctx, topic, 0, ch)
		})
		spawn("check", parallel.Exit, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-ch:
				empty = msg == nil
				return nil
			}
		})
		return nil
	})
	return empty, err
}
