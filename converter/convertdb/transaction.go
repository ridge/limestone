package convertdb

import (
	"fmt"

	"github.com/ridge/limestone/typeddb"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
)

// Transaction is a database transaction.
//
// Implements and extends limestone.Transaction.
type Transaction struct {
	typeddb.Transaction
	tc     typeddb.TransactionControl
	db     *DB
	forget map[typeddb.EID]bool

	// Raw is the raw transaction on which this one is based.
	//
	// Do not modify.
	Raw wire.Transaction

	// Other is a map of changes that are not tracked by the transactional
	// database. Changes for untracked entity kinds, as well as for untracked
	// fields in tracked entity kinds, end up here.
	//
	// You can modify this before the transaction is committed. The Commit
	// method includes all changes from Other in the output transaction.
	Other wire.Changes
}

// Forget schedules the entity to be forgotten after the transaction is
// committed. Does not affect the handling of the current transaction.
//
// The entity will be purged from memory, and further incoming updates to it
// will be silently dropped.
//
// The typical use case is when Deleted has been set on an entity.
func (txn Transaction) Forget(obj any) {
	txn.forget[txn.db.tdb.EIDOf(obj)] = true
}

// Cancel cancels the transaction.
//
// Does nothing if already committed or canceled,
// therefore safe to use in a defer statement.
func (txn Transaction) Cancel() {
	txn.tc.Cancel()
}

// Commit commits the transaction and returns an outgoing wire transaction.
//
// In the resulting wire transaction, Changes is minimized by removing empty
// diffs and containing maps.
func (txn Transaction) Commit() wire.Transaction {
	res := txn.Raw
	res.Changes = txn.Other.Clone()

	for eid, change := range txn.tc.Changes() {
		diff := must.OK1(wire.Encode(txn.db.kinds[eid.Kind.DBName].after, change.Before, change.After, nil))
		if diff == nil {
			continue
		}

		changes := res.Changes[eid.Kind.DBName]
		if changes == nil {
			changes = wire.KindChanges{}
			res.Changes[eid.Kind.DBName] = changes
		}

		for k, v := range changes[eid.ID] {
			if diff[k] != nil {
				panic(fmt.Errorf("conflict when serializing %s in %s", k, eid))
			}
			diff[k] = v
		}
		changes[eid.ID] = diff
	}

	for eid := range txn.forget {
		txn.tc.Prune(eid)
	}
	txn.tc.Commit()

	res.Changes.Optimize()
	if len(res.Changes) == 0 {
		res.Changes = nil
	}
	return res
}

// Transaction opens a new transaction and applies the incoming wire transaction
// within it
func (db *DB) Transaction(raw wire.Transaction) Transaction {
	txn := Transaction{
		db:     db,
		forget: map[typeddb.EID]bool{},
		Raw:    raw,
		Other:  wire.Changes{},
	}
	txn.Transaction, txn.tc = db.tdb.TransactionBackdated(*raw.TS)

	for kindName, changes := range raw.Changes {
		kind := db.kinds[kindName]
		if kind == nil {
			txn.Other[kindName] = changes
			continue
		}
		otherByID := wire.KindChanges{}
		txn.Other[kindName] = otherByID

		for id, diff := range changes {
			other := diff.Clone()
			for _, f := range kind.before.Fields {
				delete(other, f.DBName)
			}
			otherByID[id] = other

			eid := typeddb.EID{Kind: kind.Kind, ID: id}
			before := txn.tc.GetByEID(eid)
			if before == nil && diff[kind.Identity().DBName] == nil {
				continue // forgotten object, drop the update
			}
			after := must.OK1(wire.Decode(kind.before, before, diff, nil))
			if after != nil {
				txn.tc.SetByEID(eid, after)
			}
		}
	}

	return txn
}
