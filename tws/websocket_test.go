package tws

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

func testPair(t *testing.T, server, client SessionFn) error {
	ctx := test.Context(t)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		l := tnet.ListenOnRandomPort()
		httpServer := thttp.NewServer(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Serve(w, r, DefaultConfig, server)
		}))

		spawn("server", parallel.Fail, httpServer.Run)
		spawn("client", parallel.Exit, func(ctx context.Context) error {
			return Dial(ctx, "ws://"+l.Addr().String(), nil, DefaultConfig, client)
		})
		return nil
	})
}

func TestConnectionClosedByClient(t *testing.T) {
	require.NoError(t, testPair(t, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		select {
		case <-ctx.Done():
			return errors.New("context closed too early")
		case _, ok := <-incoming:
			if ok {
				return errors.New("unexpected message received")
			}
			return nil
		}
	}, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		return nil
	}))
}

func TestConnectionClosedByServer(t *testing.T) {
	require.NoError(t, testPair(t, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		return nil
	}, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		select {
		case <-ctx.Done():
			return errors.New("context closed too early")
		case _, ok := <-incoming:
			if ok {
				return errors.New("unexpected message received")
			}
			return nil
		}
	}))
}

func TestCommunication(t *testing.T) {
	require.NoError(t, testPair(t, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		outgoing <- Message{Data: []byte("a")}
		test.AssertEvents(t, incoming, Message{Data: []byte("b")})
		outgoing <- Message{Data: []byte("c")}
		test.AssertEvents(t, incoming, Message{Data: []byte("d")})
		outgoing <- Message{Data: []byte("e")}
		<-incoming
		return ctx.Err()
	}, func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error {
		test.AssertEvents(t, incoming, Message{Data: []byte("a")})
		outgoing <- Message{Data: []byte("b")}
		test.AssertEvents(t, incoming, Message{Data: []byte("c")})
		outgoing <- Message{Data: []byte("d")}
		test.AssertEvents(t, incoming, Message{Data: []byte("e")})
		select {
		case outgoing <- Message{Data: []byte("f")}:
		case <-ctx.Done():
		}
		return ctx.Err()
	}))
}
