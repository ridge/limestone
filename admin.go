package limestone

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/meta"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
)

// Bootstrap initializes a Kafka-based Limestone database:
//  1. writes an administrative transaction creating the given entities into the
//     transaction log topic by the given name,
//  2. publishes a manifest referencing that topic.
//
// The administrative transaction created by Bootstrap bypassing restrictions on
// what producer can write to what section.
func Bootstrap(ctx context.Context, c kafka.Client, dbVersion int, topic string, obj ...any) error {
	logger := tlog.Get(ctx)

	if txn := adminTransaction(obj); txn != nil {
		logger.Debug("Publishing bootstrap transaction", zap.Object("txn", *txn))
		if err := client.PublishKafkaTransaction(ctx, c, topic, *txn); err != nil {
			return fmt.Errorf("failed to bootstrap Limestone: %w", err)
		}
	}

	manifest := wire.Manifest{
		Version: dbVersion,
		Topic:   topic,
	}
	logger.Debug("Publishing bootstrap manifest", zap.Object("manifest", manifest))
	if err := client.PublishKafkaManifest(ctx, c, manifest); err != nil {
		return fmt.Errorf("failed to bootstrap Limestone: %w", err)
	}
	return nil
}

func adminTransaction(obj []any) *wire.Transaction {
	cache := map[reflect.Type]meta.Struct{}
	changes := wire.Changes{}
	for _, o := range obj {
		t := reflect.TypeOf(o)
		s, ok := cache[t]
		if !ok {
			s = meta.Survey(t)
			cache[t] = s
		}
		diff := must.OK1(wire.Encode(s, nil, o, nil))
		byID := changes[s.DBName]
		if byID == nil {
			byID = wire.KindChanges{}
			changes[s.DBName] = byID
		}
		byID[reflect.ValueOf(o).FieldByIndex(s.Identity().Index).String()] = diff
	}
	if len(changes) == 0 {
		return nil
	}
	return &wire.Transaction{Changes: changes}
}
