package limestone

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/typeddb"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
)

// Run executes the Limestone reading pump.
// Call it once and use the context to signal shutdown.
//
// Always returns a non-nil error: either a context cancelation reason, or
// client.ErrContinuityBroken.
func (db *DB) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		ch := make(chan *wire.IncomingTransaction)
		spawn("connection", parallel.Fail, func(ctx context.Context) error {
			return db.connection.Run(ctx, ch)
		})
		spawn("incoming", parallel.Fail, func(ctx context.Context) error {
			return db.process(ctx, ch)
		})
		return nil
	})
}

func (db *DB) process(ctx context.Context, ch <-chan *wire.IncomingTransaction) error {
	logger := tlog.Get(ctx)

	var txn Transaction
	var tc typeddb.TransactionControl
	defer func() {
		if txn != nil {
			tc.Cancel()
		}
	}()

	defer db.readyCancel()
	defer db.scheduler.Clear()

	attention := map[typeddb.EID]bool{}
	caughtUpCount := 0
	var lastPos wire.Position

	for {
		var incoming *wire.IncomingTransaction
		select {
		case incoming = <-ch:
			if incoming == nil {
				logger.Debug("Reached hot end", zap.Any("position", lastPos))
			}
		case <-db.scheduler.Wait():
			for _, key := range db.scheduler.Get() {
				logger.Debug("Alarm", zap.Stringer("eid", key))
				attention[key] = true
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		if incoming != nil {
			lastPos = incoming.Position
			if db.source != nil && incoming.Source == *db.source && incoming.Session == db.session {
				continue // ignoring echoed transaction
			}
			if db.ready {
				logger.Debug("Handling transaction", zap.Object("txn", incoming))
			} else {
				caughtUpCount++
			}

			if txn == nil {
				txn, tc = db.incomingTransaction()
			}

			for k, byID := range incoming.Changes {
				kind := db.kinds[k]
				if kind == nil {
					continue
				}
				idName := kind.Identity().DBName
				for id, diff := range byID {
					key := typeddb.EID{Kind: kind, ID: id}
					before := tc.GetByEID(key)
					if before == nil && diff[idName] == nil {
						continue // An update for a pruned entity
					}
					after, err := wire.Decode(kind.Struct, before, diff, func(index int, v1, v2 reflect.Value) error {
						producers := kind.Fields[index].Producers
						switch {
						case incoming.Source.Producer == "":
							return nil // Empty producer means admin action: any update is accepted
						case len(producers) == 0:
							return fmt.Errorf("writing by %s is denied", incoming.Source)
						case producers[incoming.Source.Producer]:
							return nil // OK
						default:
							return fmt.Errorf("writing by %s is denied (allowed for %s)", incoming.Source, producers)
						}
					})
					if err != nil {
						panic(fmt.Errorf("failed to decode incoming transaction at %s: %w", incoming.Position, err))
					}
					if after == nil {
						continue
					}

					err = kind.ValidateRequired(after)
					if err != nil {
						panic(fmt.Errorf("failed to validate incoming transaction at %s: %w", incoming.Position, err))
					}

					if s, ok := after.(withSurvive); ok && tc.GetBeforeByEID(key) == nil && !s.Survive() {
						if before != nil {
							tc.Prune(key)
							delete(attention, key)
						}
						continue
					}

					tc.SetByEID(key, after)
					attention[key] = true
				}
			}

			if len(attention) == 0 { // no relevant changes
				tc.Cancel()
				txn = nil
			}
		} else {
			// we can get here because we received nil from the client (hot end reached),
			// or because the scheduler has fired while waiting at the hot end (incoming == nil already)
			if len(attention) != 0 {
				if txn == nil {
					txn, tc = db.incomingTransaction()
				} else {
					tc.Reset()
				}

				if db.wakeUp != nil {
					entities := make([]any, 0, len(attention))
					var eids []string
					if db.ready {
						eids = make([]string, 0, len(attention))
					}
					for key := range attention {
						entities = append(entities, tc.GetByEID(key))
						if db.ready {
							eids = append(eids, key.String())
						}
					}
					if db.ready {
						logger.Debug("Waking up", zap.Int("entities", len(entities)), zap.Strings("eids", eids))
					} else {
						logger.Debug("Waking up for the first time", zap.Int("entities", len(entities)))
					}

					if err := db.submit(ctx, tc); err != nil {
						return err
					}

					for key := range tc.Changes() {
						attention[key] = true
					}
				}

				if db.debugTap != nil {
					db.debugTap <- txn.Snapshot()
				}

				for key := range attention {
					db.postProcess(tc, key, tc.GetByEID(key), txn.Time())
				}
				attention = map[typeddb.EID]bool{}

				tc.Commit()
				txn = nil
			}

			if !db.ready {
				logger.Info("Limestone ready", zap.Int("transactions", caughtUpCount), zap.Any("position", lastPos))
				db.ready = true
				db.readyCancel()
			}
		}
	}
}

func (db *DB) incomingTransaction() (Transaction, typeddb.TransactionControl) {
	return db.tdb.Transaction()
}
