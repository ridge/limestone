package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/kafka/mock"
	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/limestone/tws"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	group *parallel.Group
	kafka kafka.Client
	addr  string
	conns int
}

func testSetup(t *testing.T) *testEnv {
	var env testEnv

	env.group = test.Group(t)

	listener := tnet.ListenOnRandomPort()
	env.addr = listener.Addr().String()

	env.kafka = mock.New()
	require.NoError(t, client.PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))

	env.group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return Run(ctx, Config{
			Listener: listener,
			Kafka:    env.kafka,
		})
	})

	return &env
}

func (env *testEnv) spawnConnection(version int, pos wire.Position, filter wire.Filter) (<-chan *wire.IncomingTransaction, <-chan error) {
	res := make(chan *wire.IncomingTransaction)
	errors := make(chan error)
	env.conns++
	env.group.Spawn(fmt.Sprintf("conn%d", env.conns), parallel.Continue, func(ctx context.Context) error {
		return tws.Dial(ctx, fmt.Sprintf("ws://%s/pull", env.addr), nil, tws.StreamerConfig, func(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) error {
			defer close(res)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case outgoing <- tws.Message{Data: must.OK1(json.Marshal(wire.Request{Version: version, Last: pos, Filter: filter}))}:
			}

			for {
				var notification wire.Notification
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg, ok := <-incoming:
					if !ok {
						return nil
					}
					must.OK(json.Unmarshal(msg.Data, &notification))
				}
				if notification.Err != "" {
					go func() { //nolint:nakedgoroutine // FIXME (dmitry): discuss what to do
						select {
						case <-env.group.Context().Done():
						case errors <- notification.Err:
						}
					}()
					return nil
				}
				if notification.Txn != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case res <- notification.Txn:
					}
				}
				if notification.Hot {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case res <- nil:
					}
				}
			}
		})
	})
	return res, errors
}

var (
	testTxn1 = wire.Transaction{
		Source:  wire.Source{Producer: "foo"},
		Session: 42,
		Changes: wire.Changes{
			"apple": wire.KindChanges{
				"a": wire.Diff{
					"Color": json.RawMessage(`"red"`),
				},
			},
		},
	}
	testTxn2 = wire.Transaction{
		Source:  wire.Source{Producer: "bar"},
		Session: 24,
		Changes: wire.Changes{
			"orange": wire.KindChanges{
				"o": wire.Diff{
					"Color": json.RawMessage(`"orange"`),
				},
			},
		},
	}
)

func TestPull(t *testing.T) {
	env := testSetup(t)

	require.NoError(t, client.PublishKafkaTransaction(env.group.Context(), env.kafka, "txlog", testTxn1))

	incoming, _ := env.spawnConnection(1, wire.Beginning, nil)

	in := <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn1.Source, in.Source)
	require.Equal(t, testTxn1.Session, in.Session)
	require.Equal(t, testTxn1.Changes, in.Changes)
	require.Nil(t, <-incoming)

	require.NoError(t, client.PublishKafkaTransaction(env.group.Context(), env.kafka, "txlog", testTxn2))

	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)

	incoming, _ = env.spawnConnection(1, wire.Position("0000000000000000-0000000000000000"), nil)

	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)

	incoming, errors := env.spawnConnection(1, wire.Position("1,0,0"), nil)
	_, ok := <-incoming
	require.False(t, ok)
	require.EqualError(t, <-errors, wire.ErrContinuityBroken.Error())

	incoming, errors = env.spawnConnection(1, wire.Position("0000000000000000-0000000000000001"), nil)
	require.Nil(t, <-incoming)

	require.NoError(t, client.PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))

	_, ok = <-incoming
	require.False(t, ok)
	require.EqualError(t, <-errors, wire.ErrContinuityBroken.Error())
}

func TestPullMismatch(t *testing.T) {
	env := testSetup(t)

	incoming, errors := env.spawnConnection(0, wire.Beginning, nil)
	_, ok := <-incoming
	require.False(t, ok)
	require.EqualError(t, <-errors, wire.ErrVersionMismatch(0, 1).Error())
}

func TestPush(t *testing.T) {
	env := testSetup(t)

	messages := make(chan *kafka.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.kafka.Read(ctx, "txlog", 0, messages)
	})
	require.Nil(t, <-messages)

	httpClient := thttp.WithRequestsLogging(&http.Client{})
	req, err := http.NewRequestWithContext(env.group.Context(), http.MethodPost, fmt.Sprintf("http://%s/push?version=1", env.addr),
		bytes.NewReader(must.OK1(json.Marshal(testTxn1))))
	require.NoError(t, err)
	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNoContent, res.StatusCode)

	msg := <-messages
	var txn wire.Transaction
	require.NoError(t, json.Unmarshal(msg.Value, &txn))
	require.Equal(t, testTxn1, txn)
	require.Nil(t, <-messages)
}

func TestPushMismatchForward(t *testing.T) {
	env := testSetup(t)

	messages := make(chan *kafka.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.kafka.Read(ctx, "txlog", 0, messages)
	})
	require.Nil(t, <-messages)

	httpClient := thttp.WithRequestsLogging(&http.Client{})
	req, err := http.NewRequestWithContext(env.group.Context(), http.MethodPost, fmt.Sprintf("http://%s/push?version=2", env.addr),
		bytes.NewReader(must.OK1(json.Marshal(testTxn1))))
	require.NoError(t, err)
	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, wire.ErrVersionMismatch(2, 1).Error(), strings.TrimSpace(string(must.OK1(io.ReadAll(res.Body)))))
}

func TestPushMismatchBackward(t *testing.T) {
	env := testSetup(t)

	messages := make(chan *kafka.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.kafka.Read(ctx, "txlog", 0, messages)
	})
	require.Nil(t, <-messages)

	httpClient := thttp.WithRequestsLogging(&http.Client{})
	req, err := http.NewRequestWithContext(env.group.Context(), http.MethodPost, fmt.Sprintf("http://%s/push?version=0", env.addr),
		bytes.NewReader(must.OK1(json.Marshal(testTxn1))))
	require.NoError(t, err)
	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusConflict, res.StatusCode)
	require.Equal(t, wire.ErrVersionMismatch(0, 1).Error(), strings.TrimSpace(string(must.OK1(io.ReadAll(res.Body)))))
}
