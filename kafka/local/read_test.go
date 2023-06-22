package local

import (
	"context"
	"os"
	"testing"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

func TestReadBeforeCreation(t *testing.T) {
	env := setupTest(t)

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 0, messages)
	})

	require.Nil(t, <-messages)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Offset":7,"Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 7,
	}, <-messages)
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 8,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadAfterCreation(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Offset":7,"Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 0, messages)
	})

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 7,
	}, <-messages)
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 8,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadAppend(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
		`FOO-666`,
	))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 0, messages)
	})

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 0,
	}, <-messages)
	require.Nil(t, <-messages)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 1,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadPartial(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
	))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 0, messages)
	})

	appendFile(env.dir+"/foo", lines(
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
	))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 0,
	}, <-messages)

	appendFile(env.dir+"/foo", lines(
		`FOO-777`,
	))
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 1,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadOffsetBeforeCreation(t *testing.T) {
	env := setupTest(t)

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 8, messages)
	})

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Offset":7,"Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 8,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadOffsetAfterCreation(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Offset":7,"Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 8, messages)
	})

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 8,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadFutureOffsetAfterCreation(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Offset":7,"Key":"666","Len":7}`,
		`FOO-666`,
	))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "foo", 8, messages)
	})

	require.Nil(t, <-messages)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "777",
			Value: []byte("FOO-777"),
		},
		Time:   testTime,
		Offset: 8,
	}, <-messages)
	require.Nil(t, <-messages)
}

func TestReadContinuityBroken(t *testing.T) {
	env := setupTest(t)

	res := make(chan error)
	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Continue, func(ctx context.Context) error {
		res <- env.client.Read(ctx, "foo", 0, messages)
		return ctx.Err()
	})

	require.Nil(t, <-messages)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
		`FOO-666`,
	))

	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic: "foo",
			Key:   "666",
			Value: []byte("FOO-666"),
		},
		Time:   testTime,
		Offset: 0,
	}, <-messages)
	require.Nil(t, <-messages)

	_ = os.Remove(env.dir + "/foo")
	require.Equal(t, api.ErrContinuityBroken, <-res)
}
