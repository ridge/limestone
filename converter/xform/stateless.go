package xform

import (
	"fmt"

	"github.com/ridge/limestone/wire"
	"go.uber.org/zap"
)

// ForEach is a shorthand for writing a typical stateless upgrade
// transformation.
//
// It returns an upgrade transformation that takes a stateless conversion and
// applies it to each transaction in the stream. Transactions that end up empty
// after the conversion are omitted from the output.
//
// Example:
//
//	var Transform = xform.ForEach(func(logger *zap.Logger, txn *wire.Transaction) {
//	    xform.DeleteEntity(txn.Changes, "old_entity")
//	    xform.DeleteFields(txn.Changes, "another_entity", "Field1", "Field2")
//	})
func ForEach(conv func(logger *zap.Logger, txn *wire.Transaction)) Transformation {
	return func(logger *zap.Logger, source <-chan wire.Transaction, dest chan<- wire.Transaction) {
		for txn := range source {
			conv(logger, &txn)
			txn.Changes.Optimize()
			if len(txn.Changes) != 0 {
				dest <- txn
			}
		}
	}
}

// DeleteEntity deletes an entity type by its DB name
// (name=... in limestone.Meta definitions)
func DeleteEntity(changes wire.Changes, typeName string) {
	delete(changes, typeName)
}

// RenameEntity changes the DB name of an entity type
// (name=... in limestone.Meta definitions)
func RenameEntity(changes wire.Changes, typeNameOld, typeNameNew string) {
	if byID, ok := changes[typeNameOld]; ok {
		if changes[typeNameNew] != nil {
			panic(fmt.Errorf("entity type %s already exists in the transaction", typeNameNew))
		}
		changes[typeNameNew] = byID
		delete(changes, typeNameOld)
	}
}

// DeleteFields deletes the given fields from an entity type
func DeleteFields(changes wire.Changes, typeName string, fieldName ...string) {
	for _, diff := range changes[typeName] {
		for _, f := range fieldName {
			delete(diff, f)
		}
	}
}

// RenameField renames the given field in an entity type
func RenameField(changes wire.Changes, typeName string, fieldNameOld, fieldNameNew string) {
	for id, diff := range changes[typeName] {
		if raw, ok := diff[fieldNameOld]; ok {
			if diff[fieldNameNew] != nil {
				panic(fmt.Errorf("field name %s already exists in entity %s of type %s", fieldNameNew, id, typeName))
			}
			diff[fieldNameNew] = raw
			delete(diff, fieldNameOld)
		}
	}
}

// RenameFields renames fields in an entity type by given mapping between old and new field names
func RenameFields(changes wire.Changes, typeName string, mapping map[string]string) {
	for id, diff := range changes[typeName] {
		for oldFieldName, newFieldName := range mapping {
			if raw, ok := diff[oldFieldName]; ok {
				if diff[newFieldName] != nil {
					panic(fmt.Errorf("field name %s already exists in entity %s of type %s", newFieldName, id, typeName))
				}
				diff[newFieldName] = raw
				delete(diff, oldFieldName)
			}
		}
	}
}

// ReplaceStringValue replaces the given string value of the given field in an entity type
func ReplaceStringValue(changes wire.Changes, typeName, fieldName, oldValue, newValue string) {
	for _, diff := range changes[typeName] {
		if raw, ok := diff[fieldName]; ok {
			if String(raw) == oldValue {
				diff[fieldName] = Marshal(newValue)
			}
		}
	}
}

// ReplaceStringValues replaces the string values of the given field in an entity type by given mapping
func ReplaceStringValues(changes wire.Changes, typeName, fieldName string, mapping map[string]string) {
	for _, diff := range changes[typeName] {
		if raw, ok := diff[fieldName]; ok {
			if newValue, ok := mapping[String(raw)]; ok {
				diff[fieldName] = Marshal(newValue)
			}
		}
	}
}
