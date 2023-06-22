package typeddb

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/must/v2"
	"time"
)

// TransactionControl is an extended manager for transaction, available only
// from the transaction constructor.
type TransactionControl struct {
	txn *transaction
}

// Cancel cancels the corresponding transaction.
// Safe to use on a transaction that has already been committed or canceled.
// Do not use the transaction after it's been canceled.
func (tc TransactionControl) Cancel() {
	tc.txn.txn.Abort()
}

// Commit commits the corresponding transaction.
// Do not use the transaction after it's been committed.
func (tc TransactionControl) Commit() {
	tc.txn.txn.Commit()
}

// Changes returns a list of changes accumulated in transaction
func (tc TransactionControl) Changes() map[EID]Change {
	return tc.txn.changes
}

// Reset makes it look as if the transaction was opened just now:
// 1. Clears a list of changes that already happened in the transaction.
// 2. Sets the time that will be returned by `txn.Time()` to the current time.
func (tc TransactionControl) Reset() {
	tc.txn.changes = map[EID]Change{}
	tc.txn.ts = time.Now()
}

// Prune removes an element from the in-memory DB
func (tc TransactionControl) Prune(eid EID) {
	must.OK1(tc.txn.txn.DeleteAll(eid.Kind.DBName, "id", eid.ID))
	delete(tc.txn.changes, eid)
}

// SetByEID sets the value of the entity in the database using eid for addressing
func (tc TransactionControl) SetByEID(eid EID, obj any) {
	set(tc.txn, eid, obj)
}

// GetByEID returns an element from the database using eid to lookup. Returns
// nil if object is not found.
func (tc TransactionControl) GetByEID(eid EID) any {
	return get(tc.txn.txn, eid)
}

// GetBeforeByEID returns the pre-transaction state of an element from the
// database using eid to lookup. Returns nil if object is not found.
func (tc TransactionControl) GetBeforeByEID(eid EID) any {
	return get(tc.txn.before.txn, eid)
}

// Annotations returns the map of annotations added to the transaction. Don't
// change it. Adding more annotations to the transaction affects the map
// returned by Annotations.
func (tc TransactionControl) Annotations() map[string]string {
	return tc.txn.annotations
}

// TypedDB is a typed wrapper around MemDB
type TypedDB struct {
	byStructType map[reflect.Type]*Kind
	memdb        *memdb.MemDB
}

func generateMemDBSchema(entities []*Kind) *memdb.DBSchema {
	tables := map[string]*memdb.TableSchema{}

	for _, kind := range entities {
		tables[kind.DBName] = &memdb.TableSchema{Name: kind.DBName, Indexes: kind.indexSchema}
	}

	return &memdb.DBSchema{Tables: tables}
}

// New creates a new typed wrapper over MemDB
func New(kinds []*Kind) *TypedDB {
	tdb := &TypedDB{
		byStructType: map[reflect.Type]*Kind{},
	}
	for _, kind := range kinds {
		if tdb.byStructType[kind.Type] != nil {
			panic(fmt.Sprintf("duplicate entity type: %v", kind.Type))
		}
		tdb.byStructType[kind.Type] = kind
	}
	tdb.memdb = must.OK1(memdb.NewMemDB(generateMemDBSchema(kinds)))
	return tdb
}

func (tdb *TypedDB) kindOf(obj any) *Kind {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	kind := tdb.byStructType[t]
	if kind == nil {
		panic(fmt.Sprintf("unexpected type: %T", obj))
	}
	return kind
}

func (tdb *TypedDB) kindOfPtr(ptr any) *Kind {
	t := reflect.TypeOf(ptr)
	if t.Kind() != reflect.Ptr {
		panic("pointer expected")
	}
	kind := tdb.byStructType[t.Elem()]
	if kind == nil {
		panic(fmt.Sprintf("unexpected struct type: %v", t))
	}
	return kind
}

// EIDOf returns the EID for an entity structure given by value or pointer. The
// entity need not be present in the database.
func (tdb *TypedDB) EIDOf(obj any) EID {
	kind := tdb.kindOf(obj)
	r := reflect.Indirect(reflect.ValueOf(obj))
	idField := kind.Identity()
	id := r.FieldByIndex(idField.Index).String()
	if id == "" {
		panic(fmt.Sprintf("%s.%s is not set", kind, idField.GoName))
	}
	return EID{Kind: kind, ID: id}
}

// Snapshot returns a new r/o snapshot of the database.
func (tdb *TypedDB) Snapshot() Snapshot {
	r := tdb.memdb.Txn(false)
	return &snapshot{tdb: tdb, txn: r, ts: time.Now()}
}

// Transaction returns a writable transaction over the DB,
// along with the control interface
func (tdb *TypedDB) Transaction() (Transaction, TransactionControl) {
	w := tdb.memdb.Txn(true)
	return tdb.transaction(w, time.Now()) // time taken after acquiring the transaction mutex
}

// TransactionBackdated is a version of Transaction that allows the caller
// to override the transaction timestamp that's made visible as `txn.Time()`.
//
// Useful for history-analyzing applications.
func (tdb *TypedDB) TransactionBackdated(ts time.Time) (Transaction, TransactionControl) {
	w := tdb.memdb.Txn(true)
	return tdb.transaction(w, ts)
}

func (tdb *TypedDB) transaction(w *memdb.Txn, ts time.Time) (Transaction, TransactionControl) {
	r := tdb.memdb.Txn(false) // r/o snapshot at the same moment

	txn := &transaction{
		snapshot:    snapshot{tdb: tdb, txn: w, ts: ts},
		changes:     map[EID]Change{},
		before:      snapshot{tdb: tdb, txn: r, ts: ts},
		annotations: map[string]string{},
	}
	return txn, TransactionControl{txn}
}
