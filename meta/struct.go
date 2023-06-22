package meta

import (
	"fmt"
	"reflect"
)

// Meta is a type for dummy fields bearing tags for the containing structure
type Meta struct{}

var metaType = reflect.TypeOf(Meta{})

// Field describes a leaf structure field (not an embedded substructure).
// All fields are read-only.
type Field struct {
	GoName string
	DBName string
	Index  []int
	Type   reflect.Type

	Const     bool
	Required  bool
	Producers ProducerSet
}

// String returns the Go and DB field names
func (f Field) String() string {
	if f.DBName != "" && f.DBName != f.GoName {
		return fmt.Sprintf("%s (%s)", f.GoName, f.DBName)
	}
	return f.GoName
}

// Struct describes a structure.
// All fields are read-only.
type Struct struct {
	DBName   string
	Type     reflect.Type
	Fields   []Field
	identity int // index into Fields
}

const noIdentity = -1

// String returns the Go and DB type names
func (s Struct) String() string {
	return fmt.Sprintf("%s (%s)", s.Type, s.DBName)
}

// Identity returns the structure's identity field
func (s Struct) Identity() Field {
	return s.Fields[s.identity]
}

// Field finds the field with a given Go name (if exists and is unique)
func (s Struct) Field(name string) (Field, bool) {
	// check if the Go name exists and is unique
	f, ok := s.Type.FieldByName(name)
	if !ok {
		return Field{}, false
	}
	for _, field := range s.Fields {
		if field.GoName == name {
			return field, true
		}
	}
	// skipped field: return anyway to allow indices on skipped fields
	return Field{GoName: name, Index: f.Index, Type: f.Type}, true
}
