package xform

import (
	"github.com/ridge/limestone/wire"
	"go.uber.org/zap"
)

// Transformation is a database upgrade or other cleanup procedure that takes a
// sequence of transactions as input and produces a sequence of transactions as
// output. The function should read all transactions from source until the end
// of stream and write the converted stream into dest. Do not close dest.
//
// The transformation may pass transactions without change, modify them,
// introduce novel transactions, drop, join, split and reorder transactions.
//
// All transactions written into dest must have their TS field set, and these
// must be in a strictly ascending order.
//
// Panic to fail the transformation.
type Transformation func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction)
