package meta

import (
	"fmt"
	"reflect"
)

// ValidateRequired checks if all required fields of the entity are filled
// and returns an error if not
func (s Struct) ValidateRequired(entity any) error {
	v := reflect.ValueOf(entity)
	if v.Type() != s.Type {
		panicf("expected struct type %v", s.Type)
	}
	for _, field := range s.Fields {
		if !field.Required {
			continue
		}
		if v.FieldByIndex(field.Index).IsZero() {
			return fmt.Errorf("%s validation failed: missing required field %s", s, field)
		}
	}
	return nil
}
