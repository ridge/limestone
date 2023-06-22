package kafkacopy

import (
	"context"
	"testing"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/mock"
	"github.com/ridge/limestone/test"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
	"time"
)

var testTime = time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)

func TestCopy(t *testing.T) {
	ctx := test.Context(t)

	source := mock.New()
	dest := mock.New()

	require.NoError(t, source.WriteBackdated(ctx, "foo", []api.IncomingMessage{
		{
			Message: api.Message{
				Topic: "foo",
				Key:   "666",
				Value: []byte("FOO-666"),
			},
			Time: testTime,
		},
		{
			Message: api.Message{
				Topic: "foo",
				Key:   "777",
				Value: []byte("FOO-777"),
			},
			Time: testTime,
		},
	}))
	require.NoError(t, source.WriteBackdated(ctx, "bar", []api.IncomingMessage{
		{
			Message: api.Message{
				Topic:   "bar",
				Headers: map[string]string{"a": "b", "c": "d"},
				Value:   []byte("BAR"),
			},
			Time: testTime,
		},
	}))

	require.NoError(t, Run(ctx, Config{
		From:   source,
		To:     dest,
		Rename: "%s.copy",
	}))

	topics, err := dest.Topics(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"bar.copy", "foo.copy"}, topics)

	messages, err := testReadTopic(ctx, dest, "foo.copy")
	require.NoError(t, err)
	require.Equal(t, []api.IncomingMessage{
		{
			Message: api.Message{
				Topic: "foo.copy",
				Key:   "666",
				Value: []byte("FOO-666"),
			},
			Time:   testTime,
			Offset: 0,
		},
		{
			Message: api.Message{
				Topic: "foo.copy",
				Key:   "777",
				Value: []byte("FOO-777"),
			},
			Time:   testTime,
			Offset: 1,
		},
	}, messages)

	messages, err = testReadTopic(ctx, dest, "bar.copy")
	require.NoError(t, err)
	require.Equal(t, []api.IncomingMessage{
		{
			Message: api.Message{
				Topic:   "bar.copy",
				Headers: map[string]string{"a": "b", "c": "d"},
				Value:   []byte("BAR"),
			},
			Time:   testTime,
			Offset: 0,
		},
	}, messages)
}

func testReadTopic(ctx context.Context, client api.Client, topic string) ([]api.IncomingMessage, error) {
	var res []api.IncomingMessage
	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		messages := make(chan *api.IncomingMessage)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return client.Read(ctx, topic, 0, messages)
		})
		spawn("consumer", parallel.Exit, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg := <-messages:
					if msg == nil {
						return nil
					}
					res = append(res, *msg)
				}
			}
		})
		return nil
	})
	return res, err
}
