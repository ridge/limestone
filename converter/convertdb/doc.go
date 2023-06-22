// Package convertdb is a Limestone-like database specifically designed for
// history conversion tasks. The database is useful when a transformation cannot
// be applied statelessly transaction-by-transaction and needs to accumulate
// some information about the entities.
//
// # Usage
//
// When converting from an input stream of transactions to the output, the
// recommended usage pattern is to pass each incoming transaction through
// convertdb. For each input wire transaction:
//
//  1. Call db.Transaction to create a convertdb transaction for the incoming
//     wire transaction.
//
//  2. Manipulate the entities in the convertdb transaction like you would in a
//     Limestone transaction. Also, if necessary, manipulate the untracked
//     changes (see below) in txn.Other.
//
//  3. Call txn.Commit. It applies the changes to the memory database and returns
//     a wire transaction ready for the output stream.
//
//  4. If the Changes in the returned wire transaction are not nil, send the wire
//     transaction into the output stream.
//
// Example:
//
//	func Transform(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
//	    db := convertdb.New(convertdb.KindList{kindFoo, kindBar})
//	    for rawTxn := range source {
//	        txn := db.Transaction(rawTxn)
//
//	        // ...Manipulate the entities in txn...
//	        // ...Manipulate txn.Other...
//
//	        if resTxn := txn.Commit(); resTxn.Changes != nil {
//	            dest <- res
//	        }
//	    }
//	}
//
// As an alternative to txn.Commit, call txn.Cancel to discard the transaction.
// Remember to do it before calling db.Transaction again because only one
// transaction can be active at a time.
//
// There is a helper xform.ForEachWithState. It handles the common case of
// a stateful transformation that does not add, delete or reorder transactions
// and does not need to manipulate resTxn directly.
//
// Example:
//
//	var kinds = convertdb.KindList{kindFoo, kindBar}
//
//	var Transform = xform.ForEachWithState(kinds, conv func(logger *zap.Logger, txn convertdb.Transaction) bool {
//	    // ...Manipulate the entities in txn...
//	    // ...Manipulate txn.Other...
//
//	    return false // to keep the txn producer intact, true to reset it.
//	}
//
// # Entity kinds
//
// Like with Limestone, you have to define entity kinds with optional indices.
// When developing a particular upgrade step, remember that it might not be the
// last one, and the format to which it converts the data might not be current.
// For this reason, the code of your step must not depend on the current set of
// entities (*/entities). The convention is to include frozen versions of the
// required entities in subpackages under the upgrade step, with u (“upgrade”)
// as the prefix to the package name.
//
// Unlike Limestone, you don't need to include every field in your entity
// structures. Just list the fields that are being added, removed, renamed,
// changed or used read-only; in other words, the fields that matter. All other
// fields are considered untracked, and convertdb carries them over from the
// input to the output transaction. They aren't stored in memory between
// transactions. The same happens to untracked entity kinds, too.
//
// All untracked changes are kept in the txn.Other collection in the same format
// as in a Limestone transaction. You can examine and modify them before
// committing the convertdb transaction.
//
// The data types in your entity types don't have to match exactly how they are
// defined in the primary entity packages. The types can be different as long as
// they are serialization-compatible. For example, string-based types can be
// replaced with strings, and various structures with json.RawMessage. Such
// simplifications can be useful for avoiding dependencies on historical
// versions of other packages.
//
// # Common, Old and New
//
// In convertdb, you don't divide the entities into sections according to the
// component that writes them. Instead, divide them into sections according to
// their status in the conversion: use upgrade={common,old,new} in your Meta
// declaration.
//
// Example:
//
//	type Foo struct {
//	    ufoo.Common
//	    ufoo.Old
//	    ufoo.New
//	}
//
//	...
//
//	package ufoo
//
//	type ID string
//
//	type Common struct {
//	    convertdb.Meta `limestone:"name=foo,upgrade=common"`
//	    ID    ID `limestone:"identity"`
//
//	    FieldConsulted string
//	    FieldModified  string
//	}
//
//	type Old struct {
//	    convertdb.Meta `limestone:"name=foo,upgrade=old"`
//
//	    FieldRemoved string
//	}
//
//	type New struct {
//	    convertdb.Meta `limestone:"name=foo,upgrade=new"`
//
//	    FieldAdded string
//	}
//
// The in-memory database stores all fields in all sections. Incoming
// transactions are parsed into the old and common structures, and outgoing
// transactions are produced from the new and common sections.
//
// Entites need not have all three sections. Entirely new entities should only
// have a new section, and entities that are removed don't need anything beside
// an old section.
//
// # Changing the type of an existing field
//
// When an existing field changes to an incompatible data type, it would be
// natural to put a version of the field in the old and new sections, with
// different types. However, for technical reasons this is not supported, and
// would cause a panic because the entity contains fields with duplicate names.
//
// The solution is to define the field of a type that's compatible with both old
// and new values: any or json.RawMessage{}. Any JSON subtree is
// compatible with these types. While the transaction is open, change the value
// of the field.
package convertdb
