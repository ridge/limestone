package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
)

// KafkaClient is an implementation of Client that works with Kafka directly
// without going through a Limestone server
type KafkaClient struct {
	client kafka.Client
}

// NewKafkaClient creates a new Kafka-based Limestone client
func NewKafkaClient(kc kafka.Client) KafkaClient {
	return KafkaClient{client: kc}
}

type kafkaConnection struct {
	client      kafka.Client
	maintenance bool

	version int
	pos     *kafkaPosition // position just before the next message to read

	manifest      wire.Manifest
	manifestErr   error
	manifestReady chan struct{}
}

type kafkaPosition struct {
	generation int64 // implemented as the offset of the manifest record in the master topic
	offset     int64 // offset in the transaction log topic
}

const masterTopic = "limestone"

// FIXME (alexey): Remove compatibility insert (?:-0{16})? when prod is upgraded
var positionRE = regexp.MustCompile(`^([[:xdigit:]]{16})(?:-0{16})?-([[:xdigit:]]{16})$`)

func parsePosition(pos wire.Position) *kafkaPosition {
	if pos == wire.Beginning {
		return nil
	}
	m := positionRE.FindStringSubmatch(string(pos))
	if m == nil {
		return &kafkaPosition{generation: -1} // will cause ErrContinuityBroken
	}
	return &kafkaPosition{
		generation: must.OK1(strconv.ParseInt(m[1], 16, 64)),
		offset:     must.OK1(strconv.ParseInt(m[2], 16, 64)),
	}
}

func formatPosition(pos *kafkaPosition) wire.Position {
	if pos == nil {
		return wire.Beginning
	}
	return wire.Position(fmt.Sprintf("%016x-%016x", pos.generation, pos.offset))
}

// Connect implements interface Client
func (kc KafkaClient) Connect(version int, pos wire.Position, filter wire.Filter, compact bool) Connection {
	return &kafkaConnection{
		client:        kc.client,
		version:       version,
		pos:           parsePosition(pos),
		manifestReady: make(chan struct{}),
	}
}

// ConnectMaintenance is a version of Connect for use in database maintenance
// tools only.
//
// Instead of retrieving the current manifest from the database, it uses the
// given manifest as if it were current. A maintenance connection does not watch
// the database for new manifests (an ordinary connection fails when a new
// manifest is posted).
func (kc KafkaClient) ConnectMaintenance(manifest wire.Manifest, pos wire.Position, filter wire.Filter) Connection {
	return &kafkaConnection{
		client:        kc.client,
		maintenance:   true,
		version:       manifest.Version,
		pos:           parsePosition(pos),
		manifest:      manifest,
		manifestReady: make(chan struct{}),
	}
}

// ErrNoManifest is a sentinel error value returned from RetrieveManifest if no
// manifests are present and it is not asked to wait for one
var ErrNoManifest = errors.New("no manifest")

// Find and parse the last manifest before hot end.
// If the last manifest has Maintenance flag set, wait for more.
// Returns the manifest and its offset in the master topic.
// Don't try to parse any other manifests.
func lastManifest(ctx context.Context, messages <-chan *kafka.IncomingMessage, waitForManifest bool, versionAtLeast int) (wire.Manifest, int64, error) {
	var lastMsg *kafka.IncomingMessage
	for {
		select {
		case <-ctx.Done():
			return wire.Manifest{}, 0, ctx.Err()
		case msg := <-messages:
			if msg != nil {
				lastMsg = msg
				continue
			}
		}

		if lastMsg == nil { // reached hot end
			if !waitForManifest {
				return wire.Manifest{}, 0, ErrNoManifest
			}
			continue
		}

		var res wire.Manifest
		if err := json.Unmarshal(lastMsg.Value, &res); err != nil {
			return res, 0, fmt.Errorf("failed to parse manifest %q", lastMsg.Value)
		}

		if !res.Maintenance && res.Version >= versionAtLeast {
			return res, lastMsg.Offset, nil
		}

		if res.Maintenance {
			tlog.Get(ctx).Info("Database under maintenance, waiting", zap.Object("manifest", res))
		} else {
			tlog.Get(ctx).Info("Database version is too low, waiting", zap.Int("manifestVersion", res.Version), zap.Int("versionAtLeast", versionAtLeast))
		}

		lastMsg = nil
	}
}

// RetrieveManifest retrieves the current (most recent) manifest.
//
// If the current manifest has Maintenance flag set, RetrieveManifest will wait
// until a non-maintenance manifest is published.
//
// If there are no manifests, then the behaviour depends on waitForManifest
// flag: false is "return immediately", true is "wait until a non-maintenance
// manifest is published".
func (kc KafkaClient) RetrieveManifest(ctx context.Context, waitForManifest bool) (wire.Manifest, error) {
	var res wire.Manifest
	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		messages := make(chan *kafka.IncomingMessage)
		spawn("client", parallel.Fail, func(ctx context.Context) error {
			return kc.client.Read(ctx, masterTopic, 0, messages)
		})
		spawn("consumer", parallel.Exit, func(ctx context.Context) error {
			var err error
			res, _, err = lastManifest(ctx, messages, waitForManifest, -1)
			return err
		})
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("failed to retrieve manifest: %w", err)
	}
	return res, nil
}

func (kc *kafkaConnection) Run(ctx context.Context, sink chan<- *wire.IncomingTransaction) error {
	logger := tlog.Get(ctx)
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		var incomingMaster chan *kafka.IncomingMessage
		if kc.maintenance {
			if kc.pos == nil {
				kc.pos = &kafkaPosition{offset: -1}
			}
		} else {
			incomingMaster = make(chan *kafka.IncomingMessage)
			spawn("master", parallel.Fail, func(ctx context.Context) error {
				return kc.client.Read(ctx, masterTopic, 0, incomingMaster)
			})

			manifest, manifestOffset, err := lastManifest(ctx, incomingMaster, true, kc.version)
			if err != nil {
				return err
			}
			logger.Debug("Retrieved manifest from Kafka", zap.Object("manifest", manifest))
			kc.manifest = manifest

			if kc.manifest.Version != kc.version {
				kc.manifestErr = wire.ErrVersionMismatch(kc.version, kc.manifest.Version)
			}
			if kc.pos == nil {
				kc.pos = &kafkaPosition{generation: manifestOffset, offset: -1}
			} else if kc.pos.generation != manifestOffset {
				kc.manifestErr = wire.ErrContinuityBroken
			}
		}
		close(kc.manifestReady)
		if kc.manifestErr != nil {
			return kc.manifestErr
		}

		incoming := make(chan *kafka.IncomingMessage)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			offset := kc.pos.offset + 1
			logger.Info("Reading Kafka topic", zap.String("topic", kc.manifest.Topic), zap.Int64("offset", offset))
			return kc.client.Read(ctx, kc.manifest.Topic, offset, incoming)
		})

		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg := <-incomingMaster:
					if msg != nil {
						return wire.ErrContinuityBroken
					}
				case msg := <-incoming:
					var txn *wire.IncomingTransaction
					if msg != nil {
						kc.pos.offset = msg.Offset
						if len(msg.Value) == 0 { // tombstone/padding
							return nil
						}
						txn = &wire.IncomingTransaction{
							Position: formatPosition(kc.pos),
						}
						if err := json.Unmarshal(msg.Value, &txn.Transaction); err != nil {
							return fmt.Errorf("failed to parse incoming transaction %q: %w", msg.Value, err)
						}
						if txn.TS == nil {
							msgTime := msg.Time
							txn.TS = &msgTime
						}
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case sink <- txn:
					}
				}
			}
		})
		return nil
	})
}

func (kc *kafkaConnection) Submit(ctx context.Context, txn wire.Transaction) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-kc.manifestReady:
		if kc.manifestErr != nil {
			return kc.manifestErr
		}
	}
	return PublishKafkaTransaction(ctx, kc.client, kc.manifest.Topic, txn)
}

// PublishKafkaTransaction publishes a transaction in the given topic.
// For use in database initialization/maintenance/conversion tools only.
func PublishKafkaTransaction(ctx context.Context, client kafka.Client, topic string, txn wire.Transaction) error {
	return client.Write(ctx, topic, []kafka.Message{{
		Topic: topic,
		Value: must.OK1(json.Marshal(txn)),
	}})
}

// PublishKafkaManifest publishes a new manifest into the master Kafka topic.
// For use in database initialization/maintenance/conversion tools only.
func PublishKafkaManifest(ctx context.Context, client kafka.Client, manifest wire.Manifest) error {
	return client.Write(ctx, masterTopic, []kafka.Message{{
		Topic: masterTopic,
		Value: must.OK1(json.Marshal(manifest)),
	}})
}

// PredictPosition figures out the wire.Position that will describe the Kafka
// resume position once the imminent manifest is published. To be called after
// the last call to PublishKafkaTransaction but before PublishKafkaManifest.
func PredictPosition(ctx context.Context, client kafka.Client, manifest wire.Manifest) (wire.Position, error) {
	var err error
	var pos kafkaPosition

	pos.offset, err = client.LastOffset(ctx, manifest.Topic)
	if err != nil {
		return wire.Beginning, err
	}
	if pos.offset == 0 {
		return wire.Beginning, nil
	}
	pos.offset-- // we need the offset of the last message in the topic

	pos.generation, err = client.LastOffset(ctx, masterTopic)
	if err != nil {
		return wire.Beginning, err
	}
	// unlike pos.offset, pos.generation does not need to be decremented because
	// it's the offset of the manifest that isn't yet written

	return formatPosition(&pos), nil
}
