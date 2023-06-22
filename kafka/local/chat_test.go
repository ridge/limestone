package local

import (
	"context"
	"testing"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

func TestChat(t *testing.T) {
	env := setupTest(t)

	messagesFoo := make(chan *api.IncomingMessage)
	messagesBar := make(chan *api.IncomingMessage)

	env.group.Spawn("reader:bar", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "bar", 0, messagesBar)
	})

	require.Nil(t, <-messagesBar)

	require.NoError(t, env.client.Write(env.group.Context(), "foo", []api.Message{
		{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
	}))
	require.NoError(t, env.client.Write(env.group.Context(), "bar", []api.Message{
		{
			Topic:   "bar",
			Headers: map[string]string{"a": "b", "c": "d"},
			Value:   []byte("BAR"),
		},
	}))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic:   "bar",
			Headers: map[string]string{"a": "b", "c": "d"},
			Value:   []byte("BAR"),
		},
		Time:   testTime,
		Offset: 0,
	}, <-messagesBar)
	require.Nil(t, <-messagesBar)

	env.group.Spawn("reader:foo", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 0, messagesFoo)
	})

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 0,
	}, <-messagesFoo)
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 1,
	}, <-messagesFoo)
	require.Nil(t, <-messagesFoo)

	require.NoError(t, env.client.Write(env.group.Context(), "bar", []api.Message{
		{
			Topic: "bar",
			Value: []byte{},
		},
	}))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "bar",
			Value: []byte{},
		},
		Time:   testTime,
		Offset: 1,
	}, <-messagesBar)
	require.Nil(t, <-messagesBar)
}
