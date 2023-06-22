package limestone

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/typeddb"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
)

func (db *DB) prepareDiff(eid typeddb.EID, change typeddb.Change) wire.Diff {
	must.OK(eid.Kind.ValidateRequired(change.After))
	return must.OK1(wire.Encode(eid.Kind.Struct, change.Before, change.After, func(index int, v1, v2 reflect.Value) error {
		producers := eid.Kind.Fields[index].Producers
		switch {
		case len(producers) == 0:
			return fmt.Errorf("writing by %s is denied", *db.source)
		case !producers[db.source.Producer]:
			return fmt.Errorf("writing by %s is denied (allowed for %s)", *db.source, producers)
		}
		return nil
	}))
}

func (db *DB) prepareChanges(changes map[typeddb.EID]typeddb.Change) wire.Changes {
	res := wire.Changes{}
	for eid, change := range changes {
		diff := db.prepareDiff(eid, change)
		if diff == nil {
			continue
		}
		byID := res[eid.Kind.DBName]
		if byID == nil {
			byID = wire.KindChanges{}
			res[eid.Kind.DBName] = byID
		}
		byID[eid.ID] = diff
	}
	return res
}

func (db *DB) submit(ctx context.Context, tc typeddb.TransactionControl) error {
	changes := db.prepareChanges(tc.Changes())
	if len(changes) == 0 {
		return nil
	}
	wireTransaction := wire.Transaction{
		Source:  *db.source,
		Session: db.session,
		Changes: changes,
	}

	annotations := tc.Annotations()
	if len(annotations) != 0 {
		wireTransaction.Audit = json.RawMessage(must.OK1(json.Marshal(annotations)))
	}

	tlog.Get(ctx).Debug("Submitting transaction", zap.Object("txn", wireTransaction))
	if err := db.connection.Submit(ctx, wireTransaction); err != nil {
		return fmt.Errorf("failed to submit transaction: %w", err)
	}

	return nil
}
