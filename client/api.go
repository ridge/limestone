package client

import (
	"context"

	"github.com/ridge/limestone/wire"
)

// A Connection represents a logical connection to Limestone (may or may not be
// backed by an actual network connection).
type Connection interface {
	// Submit submits an outgoing transaction and waits for it to be written
	// through.
	//
	// If called before Run has started up, blocks until then.
	//
	// May return ctx.Err() or wire.ErrMismatch.
	Submit(ctx context.Context, txn wire.Transaction) error

	// Run executes the connection. It reads transactions from the beginning of
	// the history and writes them into the sink. Each time the hot end of the
	// transaction log is reached, nil is written into the sink.
	//
	// Keeps retrying in the face of network errors.
	// Always returns a non-nil error: either ctx.Err(), or wire.ErrMismatch.
	//
	// Run may not be called twice on the same connection.
	Run(ctx context.Context, sink chan<- *wire.IncomingTransaction) error
}

// Client is a Limestone client
type Client interface {
	// Connect returns a new connection positioned at the next transaction after
	// the given position. In case of a database version mismatch, or if the
	// position turns out to be invalid (which can happen if a new manifest has
	// been postead since the position was obtained), Connection.Run will return
	// wire.ErrMismatch.
	//
	// The client may, but is not obligated to, take the specified filter into
	// account. Entities and properties not matching the filter may or may not
	// be filtered out.
	//
	// If compact is true, the consumer is willing to accept compacted history.
	// All or some of the transactions before the first hot end may be
	// compacted. The compacted transactions have TS == nil. A series of such
	// transactions must only be considered as a whole; their relative order is
	// undefined.
	Connect(version int, pos wire.Position, filter wire.Filter, compact bool) Connection
}
