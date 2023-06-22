package compare

import (
	"context"
	"reflect"

	"github.com/ridge/limestone/wire"
)

// Run consumes two streams of transactions up to and including the hot end (nil), and compares them. Each stream
// represents a different version of the database: wOld for the older version and wNew for the newer version. Then, it
// outputs the comparison results into two output streams: when all transactions were walked, those streams are closed.
//
// Comparison results consist of two different types: per transaction and per entity.
//
// per transaction: the result of the comparison between two matching transactions from the two streams. Transactions
// are matched if their timestamp is equal. A transaction may not have a match, and it is considered as an added/removed
// transaction, depends on whether wOld/wNew misses it, respectively. Comparison results is sent for each timestamp
// exactly once. Output stream for this comparison result type is outputTxn.
//
// per entity: the entities that are detected as changed. Entity is considered as changed if matching transactions
// contain an unequal diff for that entity. Entity may be detected as changed more than once, that is, same entities
// may be sent multiple times. Output stream for this comparison result type is outputEnt.
func Run(ctx context.Context, wOld, wNew <-chan *wire.IncomingTransaction, outputTxn chan<- Txn, outputDif chan<- Difference) error {
	var err error
	readOld := true
	readNew := true
	var valOld *wire.IncomingTransaction
	var valNew *wire.IncomingTransaction

	for {
		if readOld {
			if valOld, err = read(ctx, wOld); err != nil {
				return err
			}
			readOld = false
		}
		if readNew {
			if valNew, err = read(ctx, wNew); err != nil {
				return err
			}
			readNew = false
		}

		if valOld == nil && valNew == nil {
			close(outputTxn)
			close(outputDif)
			return nil
		}

		switch compareTS(valOld, valNew) {
		case 1:
			outputTxn <- Txn{Transaction: valNew.Transaction, Type: Added}
			for kind, kindChanges := range valNew.Transaction.Changes {
				for id, diff := range kindChanges {
					outputDif <- Difference{
						ID:      id,
						Kind:    kind,
						DiffNew: diff,
					}
				}
			}
			readNew = true
		case 0:
			cmp := compareValues(valOld, valNew)
			if cmp == Changed {
				entities := unequalEntities(valOld.Changes, valNew.Changes)
				for _, ent := range entities {
					outputDif <- ent
				}
			}

			outputTxn <- Txn{Transaction: valNew.Transaction, Type: cmp}
			readOld = true
			readNew = true
		case -1:
			outputTxn <- Txn{Transaction: valOld.Transaction, Type: Removed}
			readOld = true
			for kind, kindChanges := range valOld.Transaction.Changes {
				for id, diff := range kindChanges {
					outputDif <- Difference{
						ID:      id,
						Kind:    kind,
						DiffOld: diff,
					}
				}
			}
		default:
			panic("unreachable")
		}
	}
}

// Type of the compare result
type Type string

// Representing type of the compare result
const (
	Added     Type = "added"
	Changed   Type = "changed"
	Removed   Type = "removed"
	Unchanged Type = "unchanged"
)

// Txn holds the transaction together with its classification
type Txn struct {
	wire.Transaction
	Type
}

// Difference between two transaction's wire.Diff (for the same entity)
type Difference struct {
	ID      string
	Kind    string
	DiffOld wire.Diff
	DiffNew wire.Diff
}

func read(ctx context.Context, incoming <-chan *wire.IncomingTransaction) (*wire.IncomingTransaction, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-incoming:
		return v, nil
	}
}

// compare timestamps of two transactions: if txn1 later than txn2, then return 1.
// treats the nil txn as if it comes after the latest possible date.
func compareTS(txn1, txn2 *wire.IncomingTransaction) int {
	switch {
	case txn1 == nil && txn2 == nil:
		panic("unreachable")
	case txn1 == nil:
		return 1
	case txn2 == nil:
		return -1
	default:
		return txn1.TS.Compare(*txn2.TS)
	}
}

// compare transactions values and return diff classification
func compareValues(txn1, txn2 *wire.IncomingTransaction) Type {
	if reflect.DeepEqual(txn1.Transaction, txn2.Transaction) {
		return Unchanged
	}
	return Changed
}

// return slice of diffs that their change is not equal between changesOld and changesNew. Order matters.
func unequalEntities(changesOld wire.Changes, changesNew wire.Changes) []Difference {
	diffs := map[string]Difference{}
	for kind, kindChanges := range changesOld {
		if changesNew[kind] == nil {
			for id, diff := range kindChanges {
				if _, ok := diffs[id]; !ok {
					diffs[id] = Difference{
						ID:      id,
						Kind:    kind,
						DiffOld: diff,
					}
				}
			}
			continue
		}
		for id, diff := range kindChanges {
			if _, ok := diffs[id]; ok {
				continue
			}
			if !reflect.DeepEqual(diff, changesNew[kind][id]) {
				diffs[id] = Difference{
					ID:      id,
					Kind:    kind,
					DiffOld: changesOld[kind][id],
					DiffNew: changesNew[kind][id],
				}
			}
		}
	}
	for kind, kindChanges := range changesNew {
		if changesOld[kind] == nil {
			for id, diff := range kindChanges {
				if _, ok := diffs[id]; !ok {
					diffs[id] = Difference{
						ID:      id,
						Kind:    kind,
						DiffNew: diff,
					}
				}
			}
			continue
		}
		for id, diff := range kindChanges {
			if _, ok := diffs[id]; ok {
				continue
			}
			if !reflect.DeepEqual(diff, changesOld[kind][id]) {
				diffs[id] = Difference{
					ID:      id,
					Kind:    kind,
					DiffNew: diff,
				}
			}
		}
	}
	out := make([]Difference, 0, len(diffs))
	for _, ent := range diffs {
		out = append(out, ent)
	}
	return out
}
