package client

import (
	"context"
	"testing"

	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

func TestSplitter(t *testing.T) {
	env := kafkaTestSetup(t)
	splitter := NewSplitter(env.client)
	conn1 := splitter.Connect(0, wire.Beginning, nil, false)
	conn2 := splitter.Connect(0, wire.Beginning, nil, false)
	incoming1 := make(chan *wire.IncomingTransaction)
	incoming2 := make(chan *wire.IncomingTransaction)
	env.group.Spawn("conn1", parallel.Fail, func(ctx context.Context) error {
		return conn1.Run(ctx, incoming1)
	})
	env.group.Spawn("conn2", parallel.Fail, func(ctx context.Context) error {
		return conn2.Run(ctx, incoming2)
	})
	env.group.Spawn("splitter", parallel.Fail, splitter.Run)

	require.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Topic: "txlog"}))
	require.Nil(t, <-incoming1)
	require.Nil(t, <-incoming2)

	require.NoError(t, conn1.Submit(env.group.Context(), testTxn1))
	in1 := <-incoming1
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in1.Position)
	require.NotZero(t, in1.TS)
	require.Equal(t, testTxn1.Source, in1.Source)
	require.Equal(t, testTxn1.Session, in1.Session)
	require.Equal(t, testTxn1.Changes, in1.Changes)
	in2 := <-incoming2
	require.Equal(t, wire.Position("0000000000000000-0000000000000000"), in2.Position)
	require.NotZero(t, in2.TS)
	require.Equal(t, testTxn1.Source, in2.Source)
	require.Equal(t, testTxn1.Session, in2.Session)
	require.Equal(t, testTxn1.Changes, in2.Changes)
	require.Equal(t, in1.TS, in2.TS)
	require.Nil(t, <-incoming1)
	require.Nil(t, <-incoming2)

	require.NoError(t, conn2.Submit(env.group.Context(), testTxn2))
	in1 = <-incoming1
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in1.Position)
	require.NotZero(t, in1.TS)
	require.Equal(t, testTxn2.Source, in1.Source)
	require.Equal(t, testTxn2.Session, in1.Session)
	require.Equal(t, testTxn2.Changes, in1.Changes)
	in2 = <-incoming2
	require.Equal(t, wire.Position("0000000000000000-0000000000000001"), in2.Position)
	require.NotZero(t, in2.TS)
	require.Equal(t, testTxn2.Source, in2.Source)
	require.Equal(t, testTxn2.Session, in2.Session)
	require.Equal(t, testTxn2.Changes, in2.Changes)
	require.Equal(t, in1.TS, in2.TS)
	require.Nil(t, <-incoming1)
	require.Nil(t, <-incoming2)
}

func TestFilterUnion(t *testing.T) {
	f1 := wire.Filter{
		"foo": nil,
		"bar": []string{"a", "b"},
		"baz": []string{"z"},
		"qux": []string{"y"},
	}
	f2 := wire.Filter{
		"foo": []string{"x"},
		"bar": []string{"b", "c"},
		"baz": nil,
		"zap": []string{"w"},
	}

	var f wire.Filter
	filterUnion(&f, f1)
	require.Nil(t, f)

	f = f1
	filterUnion(&f, nil)
	require.Nil(t, f)

	f = f1
	filterUnion(&f, f1)
	require.Equal(t, f1, f)

	filterUnion(&f, f2)
	require.Equal(t, wire.Filter{
		"foo": nil,
		"bar": []string{"a", "b", "c"},
		"baz": nil,
		"qux": []string{"y"},
		"zap": []string{"w"},
	}, f)
}
