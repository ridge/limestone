package wire

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/meta"
)

// Decode deserializes a database update by applying the difference to the
// existing entity.
//
// Returns nil if there are no effective changes. Otherwise, returns a modified
// copy of the existing entity or a new entity if existing is nil. Const fields
// can only be modified when existing is nil.
//
// If a validate function is specified, it is called for every modified field;
// it should return an error if changing the field is not allowed.
func Decode(metaStruct meta.Struct, existing any, diff Diff, validate ValidateFn) (any, error) {
	v := reflect.New(metaStruct.Type).Elem()
	z := reflect.Zero(metaStruct.Type)
	if existing != nil {
		v.Set(reflect.ValueOf(existing))
	}

	changed := false
	for i, field := range metaStruct.Fields {
		raw, ok := diff[field.DBName]
		if !ok {
			continue
		}

		f := v.FieldByIndex(field.Index)
		old := f.Interface()
		f.Set(z.FieldByIndex(field.Index)) // prevent reuse of maps and structs
		if err := json.Unmarshal(raw, f.Addr().Interface()); err != nil {
			return nil, fmt.Errorf("update decoding failed: field %s of %s: %w", field, metaStruct, err)
		}
		if reflect.DeepEqual(old, f.Interface()) {
			continue
		}
		if field.Const && existing != nil {
			return nil, fmt.Errorf("update decoding failed: const field %s of %s modified", field, metaStruct)
		}
		if validate != nil {
			if err := validate(i, reflect.ValueOf(old), f); err != nil {
				return nil, fmt.Errorf("update decoding failed: field %s of %s: %w", field, metaStruct, err)
			}
		}
		changed = true
	}

	if !changed {
		return nil, nil
	}
	return v.Interface(), nil
}
