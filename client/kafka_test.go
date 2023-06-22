package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/kafka/mock"
	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

type kafkaTestEnv struct {
	group  *parallel.Group
	kafka  kafka.Client
	client KafkaClient
	conns  int
}

func kafkaTestSetup(t *testing.T) *kafkaTestEnv {
	var env kafkaTestEnv

	env.group = test.Group(t)
	env.kafka = mock.New()
	env.client = NewKafkaClient(env.kafka)

	return &env
}

func (env *kafkaTestEnv) spawnConnection(version int, pos wire.Position, filter wire.Filter) (Connection, <-chan *wire.IncomingTransaction) {
	incoming := make(chan *wire.IncomingTransaction)
	conn := env.client.Connect(version, pos, filter, false)
	env.conns++
	env.group.Spawn(fmt.Sprintf("conn%d", env.conns), parallel.Fail, func(ctx context.Context) error {
		return conn.Run(ctx, incoming)
	})
	return conn, incoming
}

func (env *kafkaTestEnv) spawnConnectionFail(version int, pos wire.Position, filter wire.Filter) (Connection, <-chan *wire.IncomingTransaction, <-chan error) {
	res := make(chan error)
	incoming := make(chan *wire.IncomingTransaction)
	conn := env.client.Connect(version, pos, filter, false)
	env.conns++
	env.group.Spawn(fmt.Sprintf("conn%d", env.conns), parallel.Continue, func(ctx context.Context) error {
		res <- conn.Run(ctx, incoming)
		return nil
	})
	return conn, incoming, res
}

func TestKafkaClientRetrieveManifest(t *testing.T) {
	env := kafkaTestSetup(t)

	_, err := env.client.RetrieveManifest(env.group.Context(), false)
	require.True(t, errors.Is(err, ErrNoManifest))

	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))
	m, err := env.client.RetrieveManifest(env.group.Context(), true)
	require.NoError(t, err)
	require.Equal(t, wire.Manifest{Version: 1, Topic: "txlog"}, m)

	m, err = env.client.RetrieveManifest(env.group.Context(), false)
	require.NoError(t, err)
	require.Equal(t, wire.Manifest{Version: 1, Topic: "txlog"}, m)

	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 2, Topic: "txlog2"}))
	m, err = env.client.RetrieveManifest(env.group.Context(), true)
	require.NoError(t, err)
	require.Equal(t, wire.Manifest{Version: 2, Topic: "txlog2"}, m)
}

func TestKafkaClientRead(t *testing.T) {
	env := kafkaTestSetup(t)

	_, incoming := env.spawnConnection(1, wire.Beginning, nil)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))
	require.Nil(t, <-incoming)
	require.NoError(t, PublishKafkaTransaction(env.group.Context(), env.kafka, "txlog", testTxn1))
	in := <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn1.Source, in.Source)
	require.Equal(t, testTxn1.Session, in.Session)
	require.Equal(t, testTxn1.Changes, in.Changes)
	require.Nil(t, <-incoming)
	require.NoError(t, PublishKafkaTransaction(env.group.Context(), env.kafka, "txlog", testTxn2))
	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)

	// read again with another connection
	_, incoming = env.spawnConnection(1, wire.Beginning, nil)
	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn1.Source, in.Source)
	require.Equal(t, testTxn1.Session, in.Session)
	require.Equal(t, testTxn1.Changes, in.Changes)
	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)

	// read from given position
	_, incoming = env.spawnConnection(1, wire.Position("0000000000000000-0000000000000000"), nil)
	in = <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)
	_, incoming = env.spawnConnection(1, wire.Position("0000000000000000-0000000000000001"), nil)
	require.Nil(t, <-incoming)
}

func TestKafkaClientOldPosition(t *testing.T) {
	env := kafkaTestSetup(t)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))

	_, incoming, res := env.spawnConnectionFail(1, wire.Position("0,0,0"), nil)
	require.Equal(t, wire.ErrContinuityBroken, <-res)
	require.Empty(t, incoming)
}

func TestKafkaClientSubmit(t *testing.T) {
	env := kafkaTestSetup(t)
	conn, _ := env.spawnConnection(1, wire.Beginning, nil)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))

	msg := make(chan *kafka.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.kafka.Read(ctx, "txlog", 0, msg)
	})

	require.Nil(t, <-msg)
	require.NoError(t, conn.Submit(env.group.Context(), testTxn1))
	raw := <-msg
	var in wire.Transaction
	require.NoError(t, json.Unmarshal(raw.Value, &in))
	require.Equal(t, testTxn1, in)
	require.Nil(t, <-msg)
}

func TestKafkaClientEcho(t *testing.T) {
	env := kafkaTestSetup(t)
	conn, incoming := env.spawnConnection(1, wire.Beginning, nil)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))
	require.Nil(t, <-incoming)

	require.NoError(t, conn.Submit(env.group.Context(), testTxn1))
	in := <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn1.Source, in.Source)
	require.Equal(t, testTxn1.Session, in.Session)
	require.Equal(t, testTxn1.Changes, in.Changes)
	require.Nil(t, <-incoming)
}

func TestKafkaClientDerail(t *testing.T) {
	env := kafkaTestSetup(t)
	conn, incoming, res := env.spawnConnectionFail(1, wire.Beginning, nil)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog"}))
	require.Nil(t, <-incoming)
	require.NoError(t, conn.Submit(env.group.Context(), testTxn1))
	in := <-incoming
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn1.Source, in.Source)
	require.Equal(t, testTxn1.Session, in.Session)
	require.Equal(t, testTxn1.Changes, in.Changes)
	require.Nil(t, <-incoming)

	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 1, Topic: "txlog2"}))
	require.Equal(t, wire.ErrContinuityBroken, <-res)

	_, incoming, res = env.spawnConnectionFail(1, wire.Position("0000000000000000-0000000000000000"), nil)
	require.Equal(t, wire.ErrContinuityBroken, <-res)
	require.Empty(t, incoming)

	conn, incoming = env.spawnConnection(1, wire.Beginning, nil)
	require.Nil(t, <-incoming)
	require.NoError(t, conn.Submit(env.group.Context(), testTxn2))
	in = <-incoming
	require.Equal(t, wire.Position("0000000000000001-0000000000000000"), in.Position)
	require.NotZero(t, in.TS)
	require.Equal(t, testTxn2.Source, in.Source)
	require.Equal(t, testTxn2.Session, in.Session)
	require.Equal(t, testTxn2.Changes, in.Changes)
	require.Nil(t, <-incoming)

	_, incoming = env.spawnConnection(1, wire.Position("0000000000000001-0000000000000000"), nil)
	require.Nil(t, <-incoming)
}

func TestKafkaClientMismatch(t *testing.T) {
	env := kafkaTestSetup(t)
	conn, _, res := env.spawnConnectionFail(2, wire.Beginning, nil)
	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Version: 3, Topic: "txlog"}))
	require.Equal(t, wire.ErrVersionMismatch(2, 3), <-res)

	require.EqualError(t, conn.Submit(env.group.Context(), testTxn1), wire.ErrVersionMismatch(2, 3).Error())
}
