package typeddb

import (
	"fmt"
	"reflect"

	"github.com/ridge/must/v2"
)

// Transaction is a read-write view of the database.
// Do not use concurrently.
type Transaction interface {
	Snapshot
	// Set sets the value of the entity in the database using obj type and ID field for addressing.
	// It's safe and cheap to call it without making any changes to the entity.
	Set(obj any)
	// Before returns a state of database at the time of the transaction start
	Before() Snapshot
	// Snapshot returns a read-only snapshot that includes any uncommitted changes made so far.
	// This snapshot will survive the transaction and is safe to use concurrently.
	// The snapshot inherits the transaction's timestamp.
	Snapshot() Snapshot
	// Annotate leaves an audit trail, attaching a key-value pair to the
	// transaction. The meaning of particular keys and values is
	// application-defined. The pairs are stored unordered. A later annotation
	// with the same key panics unless the value is also the same.
	Annotate(key, value string)
}

// Change is a single change done in a transaction
type Change struct {
	Before any
	After  any
}

type transaction struct {
	snapshot

	changes     map[EID]Change
	before      snapshot
	annotations map[string]string
}

func set(txn *transaction, eid EID, obj any) {
	obj = reflect.Indirect(reflect.ValueOf(obj)).Interface()
	change, exists := txn.changes[eid]
	if !exists {
		change.Before = get(txn.txn, eid)
	}
	must.OK(txn.txn.Insert(eid.Kind.DBName, obj))
	change.After = obj
	txn.changes[eid] = change
}

func (txn *transaction) Set(obj any) {
	set(txn, txn.tdb.EIDOf(obj), obj)
}

func (txn *transaction) Before() Snapshot {
	return txn.before
}

func (txn *transaction) Snapshot() Snapshot {
	return snapshot{
		tdb: txn.tdb,
		txn: txn.txn.Snapshot(),
		ts:  txn.ts,
	}
}

func (txn *transaction) Annotate(key, value string) {
	if before, ok := txn.annotations[key]; ok {
		if before != value {
			panic(fmt.Errorf("annotation %q changed from %q to %q", key, before, value))
		}
		return
	}
	txn.annotations[key] = value
}

// SetMany is a convenience function that puts all passed objects into the
// transaction. If a parameter is a slice, every slice element is separately put
// into the transaction.
func SetMany(txn Transaction, objs ...any) {
	for _, obj := range objs {
		v := reflect.ValueOf(obj)
		if v.Kind() == reflect.Slice {
			for i := 0; i < v.Len(); i++ {
				txn.Set(v.Index(i).Interface())
			}
		} else {
			txn.Set(obj)
		}
	}
}

// TransactMany is a convenience function that creates a transaction, puts all
// passed objects into it like SetMany and commits it.
func TransactMany(db *TypedDB, objs ...any) {
	txn, txnCntrl := db.Transaction()
	SetMany(txn, objs...)
	txnCntrl.Commit()
}
