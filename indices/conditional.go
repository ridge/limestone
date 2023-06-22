package indices

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type conditionalIndexDef struct {
	desc      string
	condition func(meta.Struct) func(reflect.Value) bool
	def       Definition
}

// IndexIfNonzero filters another index on the given field of the entity.
// Entities where the field has the zero value of its type are excluded from the
// index.
func IndexIfNonzero(fieldName string, def Definition) Definition {
	return conditionalIndexSimple(fieldName, false, def)
}

// IndexIfZero filters another index on the given field of the entity.
// Entities where the field has a nonzero value of its type are excluded from
// the index.
func IndexIfZero(fieldName string, def Definition) Definition {
	return conditionalIndexSimple(fieldName, true, def)
}

func conditionalIndexSimple(fieldName string, zero bool, def Definition) Definition {
	desc := fieldName
	if zero {
		desc = "!" + desc
	}
	return conditionalIndexDef{
		desc: desc,
		condition: func(s meta.Struct) func(reflect.Value) bool {
			field, ok := s.Field(fieldName)
			if !ok {
				panic(fmt.Errorf("field %s.%s not found", s.Type, fieldName))
			}
			return func(v reflect.Value) bool {
				return v.FieldByIndex(field.Index).IsZero() == zero
			}
		},
		def: def,
	}
}

// IndexIf filters another index on the condition given by a filtering function.
// The function must take the entity structure by value as its only argument,
// and return bool. Entities for which the function returns false are excluded
// from the index.
func IndexIf(desc string, filter any, def Definition) Definition {
	t := reflect.TypeOf(filter)
	if t.Kind() != reflect.Func {
		panic(fmt.Errorf("condition %s: argument is not a function", desc))
	}
	if t.NumIn() != 1 {
		panic(fmt.Errorf("condition %s: function must take exactly one argument", desc))
	}
	if t.NumOut() != 1 {
		panic(fmt.Errorf("condition %s: function must return exactly one value", desc))
	}
	if t.Out(0) != reflect.TypeOf(false) {
		panic(fmt.Errorf("condition %s: function must return bool", desc))
	}
	return conditionalIndexDef{
		desc: desc,
		condition: func(s meta.Struct) func(reflect.Value) bool {
			if !s.Type.AssignableTo(t.In(0)) {
				panic(fmt.Errorf("condition %s: function argument type must admit %s", desc, s.Type))
			}
			fn := reflect.ValueOf(filter)
			return func(v reflect.Value) bool {
				return fn.Call([]reflect.Value{v})[0].Bool()
			}
		},
		def: def,
	}
}

func (cid conditionalIndexDef) Name() string {
	return fmt.Sprintf("%s[%s]", cid.def.Name(), cid.desc)
}

func (cid conditionalIndexDef) Args() int {
	return cid.def.Args()
}

func (cid conditionalIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	var indexer memdb.Indexer

	schema := cid.def.Index(s)
	conditionalIndexer := conditionalIndexer{
		def:       cid,
		condition: cid.condition(s),
		indexer:   schema.Indexer,
	}

	switch rawIndexer := schema.Indexer.(type) {
	case memdb.SingleIndexer:
		ci := conditionalSingleIndexer{conditionalIndexer: conditionalIndexer}
		indexer = ci
		if _, ok := rawIndexer.(memdb.PrefixIndexer); ok {
			indexer = conditionalSinglePrefixIndexer{conditionalSingleIndexer: ci}
		}
	case memdb.MultiIndexer:
		ci := conditionalMultiIndexer{conditionalIndexer: conditionalIndexer}
		indexer = ci
		if _, ok := rawIndexer.(memdb.PrefixIndexer); ok {
			indexer = conditionalMultiPrefixIndexer{conditionalMultiIndexer: ci}
		}
	default:
		panic("invalid indexer")
	}

	return &memdb.IndexSchema{
		Name:         cid.Name(),
		AllowMissing: true,
		Unique:       schema.Unique,
		Indexer:      indexer,
	}
}

type conditionalIndexer struct {
	def       conditionalIndexDef
	condition func(reflect.Value) bool
	indexer   memdb.Indexer
}

type conditionalSingleIndexer struct {
	conditionalIndexer
}

func (csi conditionalSingleIndexer) FromArgs(args ...any) ([]byte, error) {
	return csi.indexer.FromArgs(args...)
}

func (csi conditionalSingleIndexer) FromObject(obj any) (bool, []byte, error) {
	if !csi.condition(reflect.ValueOf(obj)) {
		return false, nil, nil
	}
	return csi.indexer.(memdb.SingleIndexer).FromObject(obj)
}

type conditionalSinglePrefixIndexer struct {
	conditionalSingleIndexer
}

func (cspi conditionalSinglePrefixIndexer) PrefixFromArgs(args ...any) ([]byte, error) {
	return cspi.indexer.(memdb.PrefixIndexer).PrefixFromArgs(args...)
}

type conditionalMultiIndexer struct {
	conditionalIndexer
}

func (cmi conditionalMultiIndexer) FromArgs(args ...any) ([]byte, error) {
	return cmi.indexer.FromArgs(args...)
}

func (cmi conditionalMultiIndexer) FromObject(obj any) (bool, [][]byte, error) {
	if !cmi.condition(reflect.ValueOf(obj)) {
		return false, nil, nil
	}
	return cmi.indexer.(memdb.MultiIndexer).FromObject(obj)
}

type conditionalMultiPrefixIndexer struct {
	conditionalMultiIndexer
}

func (cmpi conditionalMultiPrefixIndexer) PrefixFromArgs(args ...any) ([]byte, error) {
	return cmpi.indexer.(memdb.PrefixIndexer).PrefixFromArgs(args...)
}
