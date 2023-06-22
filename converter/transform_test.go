package converter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/kafka/mock"
	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testEnv struct {
	ctx      context.Context
	lc       client.KafkaClient
	manifest wire.Manifest
	config   Config
}

func setupTest(t *testing.T) *testEnv {
	ctx := test.Context(t)

	kafka := mock.New()
	env := &testEnv{
		ctx: ctx,
		manifest: wire.Manifest{
			Version: 0,
			Topic:   "txlog",
		},
		config: Config{
			SourceKafka: kafka,
			DestKafka:   kafka,
			NewTopic:    "txlog.new",
		},
	}
	env.lc = client.NewKafkaClient(kafka)

	require.NoError(t, client.PublishKafkaTransaction(ctx, kafka, "txlog", wire.Transaction{
		Source:  wire.Source{Producer: "foo"},
		Session: 42,
		Changes: wire.Changes{
			"apple": wire.KindChanges{
				"r": wire.Diff{
					"Color": json.RawMessage(`"red"`),
				},
			},
		},
	}))
	require.NoError(t, client.PublishKafkaTransaction(ctx, kafka, "txlog", wire.Transaction{
		Source:  wire.Source{Producer: "bar"},
		Session: 24,
		Changes: wire.Changes{
			"apple": wire.KindChanges{
				"o": wire.Diff{
					"Color": json.RawMessage(`"orange"`),
				},
			},
		},
	}))
	require.NoError(t, client.PublishKafkaManifest(ctx, kafka, env.manifest))

	return env
}

func (env *testEnv) output(t *testing.T) []wire.Transaction {
	var res []wire.Transaction
	require.NoError(t, parallel.Run(env.ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		incoming := make(chan *wire.IncomingTransaction)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return env.lc.Connect(0, wire.Beginning, nil, false).Run(ctx, incoming)
		})
		spawn("consumer", parallel.Exit, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case txn := <-incoming:
					if txn == nil {
						return nil
					}
					require.NotEmpty(t, txn.TS)
					txn.TS = nil // for ease of comparison to expected values
					res = append(res, txn.Transaction)
				}
			}
		})
		return nil
	}))
	return res
}

func TestTransformEmpty(t *testing.T) {
	env := setupTest(t)

	require.NoError(t, transform(env.ctx, env.config, env.lc, env.manifest, nil))

	require.Equal(t, []wire.Transaction{
		{
			Source:  wire.Source{Producer: "foo"},
			Session: 42,
			Changes: wire.Changes{
				"apple": wire.KindChanges{
					"r": wire.Diff{
						"Color": json.RawMessage(`"red"`),
					},
				},
			},
		},
		{
			Source:  wire.Source{Producer: "bar"},
			Session: 24,
			Changes: wire.Changes{
				"apple": wire.KindChanges{
					"o": wire.Diff{
						"Color": json.RawMessage(`"orange"`),
					},
				},
			},
		},
	}, env.output(t))
}

func TestTransformChainNotOK(t *testing.T) {
	env := setupTest(t)

	require.NoError(t, transform(env.ctx, env.config, env.lc, env.manifest, []step{
		{
			name: "apple-color-to-tone",
			run: func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
				for txn := range source {
					apple := txn.Changes["apple"]
					for _, diff := range apple {
						if color := diff["Color"]; color != nil {
							delete(diff, "Color")
							diff["Tone"] = color
						}
					}
					dest <- txn
				}
			},
		},
		{
			name: "apple-tone-value-to-uppercase",
			run: func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
				for txn := range source {
					apple := txn.Changes["apple"]
					for _, diff := range apple {
						if tone := diff["Tone"]; tone != nil {
							var value string
							require.NoError(t, json.Unmarshal(tone, &value))
							diff["Tone"] = json.RawMessage(must.OK1(json.Marshal(strings.ToUpper(value))))
						}
					}
					dest <- txn
				}
			},
		},
	}), nil)

	require.Equal(t, []wire.Transaction{
		{
			Source:  wire.Source{Producer: "foo"},
			Session: 42,
			Changes: wire.Changes{
				"apple": wire.KindChanges{
					"r": wire.Diff{
						"Tone": json.RawMessage(`"RED"`),
					},
				},
			},
		},
		{
			Source:  wire.Source{Producer: "bar"},
			Session: 24,
			Changes: wire.Changes{
				"apple": wire.KindChanges{
					"o": wire.Diff{
						"Tone": json.RawMessage(`"ORANGE"`),
					},
				},
			},
		},
	}, env.output(t))
}

func TestTransformChainError(t *testing.T) {
	env := setupTest(t)

	err := transform(env.ctx, env.config, env.lc, env.manifest, []step{
		{
			name: "apple-color-to-tone",
			run: func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
				for txn := range source {
					apple := txn.Changes["apple"]
					for _, diff := range apple {
						if color := diff["Color"]; color != nil {
							delete(diff, "Color")
							diff["Tone"] = color
						}
					}
					dest <- txn
				}
			},
		},
		{
			name: "fail-on-second-txn",
			run: func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
				txn, ok := <-source
				if !ok {
					return
				}
				dest <- txn

				_, ok = <-source
				if ok {
					panic("transformation failed")
				}
			},
		},
		{
			name: "apple-tone-value-to-uppercase",
			run: func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
				for txn := range source {
					apple := txn.Changes["apple"]
					for _, diff := range apple {
						if tone := diff["Tone"]; tone != nil {
							var value string
							must.OK(json.Unmarshal(tone, &value))
							diff["Tone"] = json.RawMessage(must.OK1(json.Marshal(strings.ToUpper(value))))
						}
					}
					dest <- txn
				}
			},
		},
	})
	require.Error(t, err)
	require.Regexp(t, `^panic in conversion step fail-on-second-txn: transformation failed\n\ngoroutine`, err.Error())
}
