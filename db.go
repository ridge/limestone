package limestone

import (
	"context"
	"fmt"

	"time"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/meta"
	"github.com/ridge/limestone/scheduler"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/typeddb"
	"github.com/ridge/limestone/wire"
	"go.uber.org/zap"
)

// Snapshot is a read-only snapshot of the database.
// Safe for concurrent use.
type Snapshot = typeddb.Snapshot

// Transaction is a read/write transaction over the database.
// Do not use concurrently.
type Transaction = typeddb.Transaction

// Kind describes a particular type of limestone entity.
type Kind = typeddb.Kind

// KindList is a shortcut for declaring a list of kinds to create Config
type KindList = []*Kind

// KindOf creates a Kind from an object example and index definitions
var KindOf = typeddb.KindOf

// MustGet returns an object from the database like Snapshot.Get, panic()ing if
// the object is not found
func MustGet[T any](s Snapshot, id any, ptr *T) {
	typeddb.MustGet(s, id, ptr)
}

// Producer is a generic service name
type Producer = meta.Producer

// Source describes the submitter of a database update
type Source = wire.Source

// Convenience reexports from indices package.
// See package documentation for lib/limestone/indices.
var (
	Index       = indices.Index
	UniqueIndex = indices.UniqueIndex
	CustomIndex = indices.CustomIndex
	IndexIf     = indices.IndexIf
)

// Client is the Limestone client interface
type Client = client.Client

// Meta is a type for dummy fields bearing tags for the containing structure
type Meta = meta.Meta

// NoDeadline is a handy marker to return from Deadline() to signal "no deadline"
var NoDeadline = time.Time{}

// WithDeadline is an entity that has a deadline
type WithDeadline interface {
	Deadline() time.Time
}

type withSurvive interface {
	Survive() bool
}

// WakeUpFn is a callback type for WakeUp callback
type WakeUpFn func(ctx context.Context, txn Transaction, entities []any)

// Config is configuration of DB synchronization
type Config struct {
	// DBHistory is the list of schema updates
	DBHistory []string

	// Client is the Limestone client
	Client Client

	// Entities is a list of entities to be synchronized
	Entities KindList

	// WakeUp is a callback to be called when a batch of changes has been
	// received, and business logic can continue. Can be nil.
	//
	// Do not use the transaction passed to WakeUp from other goroutines or
	// after WakeUp returns.
	WakeUp WakeUpFn

	// Source is the info about the local service.
	// Empty iff the service is read only.
	Source

	// Logger is a logger to be used for logging in DB sync.
	Logger *zap.Logger

	// The following field is to be left empty except in tests
	Session int64

	// If DebugTap is non-nil, Limestone will send a snapshot into this channel
	// every time a transaction is committed. This applies both to Do/DoE
	// transactions and to those caused by incoming transactions. If WakeUp
	// makes changes, the snapshot includes them.
	//
	// For use in tests only. If writing to the channel blocks, this holds up
	// the entire Limestone.
	DebugTap chan<- Snapshot
}

// DBReadWrite is an abstraction for limestone DB that doesn't react to outside transactions
type DBReadWrite interface {
	WaitReady(ctx context.Context) error
	Snapshot() typeddb.Snapshot
	Do(fn func(txn Transaction))
	DoE(func(txn Transaction) error) error
}

// DBReadOnly is an abstraction for limestone DB for read-only DB operations
type DBReadOnly interface {
	Snapshot() typeddb.Snapshot
}

// DB is an instance of Limestone
type DB struct {
	tdb   *typeddb.TypedDB
	kinds map[string]*typeddb.Kind

	wakeUp    WakeUpFn
	scheduler *scheduler.Scheduler

	connection client.Connection

	source  *Source
	session int64

	// readyCtx is used as a "fence" synchronization primitive,
	// not as a context, so it is stored in this struct
	readyCtx    context.Context //nolint:containedctx
	readyCancel context.CancelFunc
	ready       bool

	logger             *zap.Logger
	monitoringInstance string
	debugTap           chan<- Snapshot
}

// New creates a new Limestone instance
func New(config Config) *DB {
	db := DB{
		tdb:       typeddb.New(config.Entities),
		kinds:     map[string]*typeddb.Kind{},
		wakeUp:    config.WakeUp,
		scheduler: scheduler.New(),
		session:   config.Session,
		logger:    config.Logger,
		debugTap:  config.DebugTap,
	}

	db.readyCtx, db.readyCancel = context.WithCancel(context.Background())

	if db.session == 0 {
		db.session = time.Now().UnixNano()
	}

	if config.Producer != "" {
		db.source = new(Source)
		*db.source = config.Source
	}

	filter := wire.Filter{}
	for _, kind := range config.Entities {
		if db.kinds[kind.DBName] != nil {
			panic(fmt.Sprintf("duplicate entity name: %s", kind.DBName))
		}
		db.kinds[kind.DBName] = kind

		fields := make([]string, 0, len(kind.Fields))
		for _, f := range kind.Fields {
			fields = append(fields, f.DBName)
		}
		filter[kind.DBName] = fields
	}
	db.connection = config.Client.Connect(len(config.DBHistory), wire.Beginning, filter, true)

	if db.monitoringInstance == "" {
		db.monitoringInstance = "main"
	}

	return &db
}

// DoE calls fn with a new transaction. The transaction is committed if fn returns no error,
// and is canceled if fn returns an error or panics.
//
// During startup, before Limestone has caught up with the hot end of the
// transaction log, DoE panics.
//
// Do not use the transaction from other goroutines or after fn returns.
func (db *DB) DoE(fn func(txn Transaction) error) error {
	if db.source == nil {
		panic("The database is read-only")
	}

	if db.readyCtx.Err() == nil || !db.ready {
		panic("Limestone is not ready")
	}

	txn, tc := db.tdb.Transaction()
	defer tc.Cancel()

	ctx := tlog.WithLogger(context.Background(), db.logger) // for logging only

	if err := fn(txn); err != nil {
		return err
	}

	snapshot := txn.Snapshot()

	if err := db.submit(ctx, tc); err != nil {
		// We cannot continue, as callers are not ready to handle this failure,
		// they only expect errors to be returned from fn() above.
		panic(fmt.Errorf("limestone cannot continue after a failure to submit transaction: %w", err))
	}

	if db.debugTap != nil {
		db.debugTap <- snapshot
	}

	for eid, change := range tc.Changes() {
		db.postProcess(tc, eid, change.After, time.Time{})
	}

	tc.Commit()
	return nil
}

// Do calls fn with a new transcation. The transaction is committed if fn returns,
// and is canceled if it panics.
//
// During startup, before the reader has caught up with the hot end of the
// transaction log, Do panics.
//
// Do not use the transaction from other goroutines or after fn returns.
func (db *DB) Do(fn func(txn Transaction)) {
	_ = db.DoE(func(txn Transaction) error {
		fn(txn)
		return nil
	})
}

func (db *DB) postProcess(tc typeddb.TransactionControl, eid typeddb.EID, obj any, now time.Time) {
	if s, ok := obj.(withSurvive); ok && !s.Survive() {
		tc.Prune(eid)
		db.schedule(eid, time.Time{})
		return
	}

	if d, ok := obj.(WithDeadline); ok {
		deadline := d.Deadline()
		if !deadline.IsZero() && now.After(deadline) {
			panic(fmt.Sprintf("entity %v returned deadline in past: %v < %v",
				eid, deadline, now))
		}
		db.schedule(eid, deadline)
	}
}

// Snapshot returns a read-only snapshot of the database.
// This is a cheap, lock-free operation.
//
// During startup, before Limestone has caught up with the hot end of the
// transaction log, Snapshot returns nil. It is therefore a good idea to call
// WaitReady during the start-up sequence to ensure that after this point
// Snapshot will never return nil.
//
// The snapshot is safe to keep indefinitely and to use concurrently.
func (db *DB) Snapshot() Snapshot {
	if db.readyCtx.Err() == nil || !db.ready {
		return nil
	}
	return db.tdb.Snapshot()
}

// WaitReady waits until Limestone has caught up. To be used in the start-up
// sequence of a service. After this point, Snapshot is guaranteed to never
// return nil.
//
// WaitReady returns an error if the context is canceled, or if Limestone is
// shut down before catch-up is complete.
//
// Safe to call concurrently.
func (db *DB) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-db.readyCtx.Done():
		if !db.ready {
			return db.readyCtx.Err()
		}
		return nil
	}
}

func (db *DB) schedule(eid typeddb.EID, when time.Time) {
	if !when.IsZero() {
		db.logger.Debug("Scheduling alarm", zap.Stringer("eid", eid), zap.Time("when", when),
			zap.Duration("left", time.Until(when)))
	}
	db.scheduler.Schedule(eid, when)
}

// SnapshotAfterTransaction returns a snapshot, except that it waits for the current transaction (if any) to finish.
// The need for this function is rare; only use it if you have a good reason.
func SnapshotAfterTransaction(db DBReadWrite) Snapshot {
	db.Do(func(Transaction) {})
	return db.Snapshot()
}
