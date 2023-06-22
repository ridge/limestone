package wire

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/meta"
)

// Encode serializes a database update by encoding the difference between
// two entities, the first of which can be nil.
//
// Returns nil if there is no difference.
//
// If a validate function is specified, it is called for every modified field;
// it should return an error if changing the field is not allowed.
func Encode(metaStruct meta.Struct, before, after any, validate ValidateFn) (Diff, error) {
	var v1 reflect.Value
	if before != nil {
		v1 = reflect.ValueOf(before)
	} else {
		v1 = reflect.Zero(metaStruct.Type)
	}
	v2 := reflect.ValueOf(after)
	if v1.Type() != metaStruct.Type || v2.Type() != metaStruct.Type {
		panic(fmt.Sprintf("expected struct type %v", metaStruct.Type))
	}

	var diff Diff
	for i, field := range metaStruct.Fields {
		f1 := v1.FieldByIndex(field.Index)
		f2 := v2.FieldByIndex(field.Index)
		i1 := f1.Interface()
		i2 := f2.Interface()
		if reflect.DeepEqual(i1, i2) {
			continue
		}
		if field.Const && before != nil {
			return nil, fmt.Errorf("update encoding failed: const field %s of %s modified", field, metaStruct)
		}
		if validate != nil {
			if err := validate(i, f1, f2); err != nil {
				return nil, fmt.Errorf("update encoding failed: field %s of %s: %w", field, metaStruct, err)
			}
		}
		raw, err := json.Marshal(i2)
		if err != nil {
			return nil, fmt.Errorf("update encoding failed: field %s of %s: %w", field, metaStruct, err)
		}
		if diff == nil {
			diff = Diff{}
		}
		diff[field.DBName] = json.RawMessage(raw)
	}
	return diff, nil
}
