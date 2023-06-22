package wire

import (
	"encoding/json"
	"reflect"
)

// A Diff represents a patch to an entity. It is a mapping from a field name to
// its new value (serialized as JSON).
type Diff map[string]json.RawMessage

// Clone returns a deep copy of the Diff
func (d Diff) Clone() Diff {
	if d == nil {
		return nil
	}
	res := Diff{}
	for k, v := range d {
		res[k] = v
	}
	return res
}

// A ValidateFn receives the index of a field within meta.Struct.Fields
// along with its old and new values, and has a chance
// to fail encoding or decoding by returning an error.
// When creating a new entity, before is a reflection of the zero value
// of the field type.
type ValidateFn func(index int, before, after reflect.Value) error
