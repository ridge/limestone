package mock

import (
	"context"

	"github.com/ridge/limestone"
	"github.com/ridge/limestone/typeddb"
)

// DBReadOnly is a read-only typeddb to be use in unittests
type DBReadOnly struct {
	DB *typeddb.TypedDB
}

// WaitReady succeeds immediately (the mock database is always ready)
func (mDB *DBReadOnly) WaitReady(ctx context.Context) error {
	return nil
}

// Snapshot returns the DB current state
func (mDB *DBReadOnly) Snapshot() limestone.Snapshot {
	return mDB.DB.Snapshot()
}

// ChangesList is an array of changes to a db. Each change consist of a map
// between a changed entity ID and the change
type ChangesList = []map[typeddb.EID]typeddb.Change

// DBReadWrite is a DBReadWrite typeddb to be use in unittests
type DBReadWrite struct {
	DBReadOnly
	changes []map[typeddb.EID]typeddb.Change
}

// NewDBReadWrite returns new instance of DBReadWrite
func NewDBReadWrite(db *typeddb.TypedDB) *DBReadWrite {
	return &DBReadWrite{
		DBReadOnly: DBReadOnly{DB: db},
		changes:    ChangesList{},
	}
}

// Changes returns the DB changes list
func (pDB *DBReadWrite) Changes() ChangesList {
	return pDB.changes
}

// Snapshot returns the DB current state
func (pDB *DBReadWrite) Snapshot() limestone.Snapshot {
	return pDB.DB.Snapshot()
}

// Do executes the function on the transaction and saves the changes
func (pDB *DBReadWrite) Do(fn func(txn limestone.Transaction)) {
	_ = pDB.DoE(func(txn limestone.Transaction) error {
		fn(txn)
		return nil
	})
}

// DoE executes the function on the transaction and saves the changes
func (pDB *DBReadWrite) DoE(fn func(txn limestone.Transaction) error) error {
	txn, txnctrl := pDB.DB.Transaction()
	defer txnctrl.Cancel()

	if err := fn(txn); err != nil {
		return err
	}

	changes := txnctrl.Changes()
	if len(changes) > 0 {
		pDB.changes = append(pDB.changes, changes)
	}
	txnctrl.Commit()

	return nil
}
