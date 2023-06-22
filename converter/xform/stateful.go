package xform

import (
	"github.com/ridge/limestone/converter/convertdb"
	"github.com/ridge/limestone/wire"
	"go.uber.org/zap"
)

// ForEachWithState is a shorthand for writing a typical stateful upgrade
// transformation.
//
// It returns an upgrade transformation that takes a stateful conversion and
// applies it to each transaction in the stream. Transactions that end up empty
// after the conversion are omitted from the output. If true is returned by the
// conversion, txn.Source will be cleared so that the transaction can bypass producer checking.
// Read the doc of convertdb in limestone/converter/convertdb/doc.go
//
// Example:
//
//	type foo struct {
//	     ufoo.Common
//	     ufoo.New
//	}
//
//	var (
//	    indexFleetID  = limestone.Index("FleetID")
//	    kindForwarder = convertdb.KindOf(forwarder{}, indexFleetID)
//	    kinds         = convertdb.KindList{kindForwarder}
//	)
//
//	var Transform = xform.ForEachWithState(func(logger *zap.Logger, txn convertdb.Transaction) bool {
//	     xform.DeleteEntity(txn.Other, "old_entity")
//	     xform.DeleteFields(txn.Other, "another_entity", "Field1", "Field2")
//
//	     var clearProducer bool
//	     for fooID, diff := range txn.Raw.Changes["foo"] {
//	          if diff["Field1"] != nil {
//	               var f foo
//	               limestone.MustGet(fooID, &f)
//	               f.Field2 = f.Field1 + f.Field3
//	               txn.Set(f)
//	               // Field1 is owned by service1 but Field2 is owned by service2, so producer has to be cleared
//	               clearProducer = true
//	          }
//	     }
//	     return clearProducer
//	})
func ForEachWithState(kinds convertdb.KindList, conv func(logger *zap.Logger, txn convertdb.Transaction) bool) Transformation {
	return func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
		db := convertdb.New(kinds)
		for rawTxn := range source {
			txn := db.Transaction(rawTxn)

			clearProducer := conv(logger, txn)
			if resTxn := txn.Commit(); len(resTxn.Changes) != 0 {
				if clearProducer {
					resTxn.Source = wire.Source{}
				}
				dest <- resTxn
			}
		}
	}
}
