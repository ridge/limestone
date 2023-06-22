package indices

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type fieldIndexDef struct {
	name       string
	skipZeros  bool
	ignoreCase bool
}

// FieldIndex specifies an index on a single field.
// The field must be of an indexable type, or a pointer to such type.
// Possible options are SkipZeros and IgnoreCase.
func FieldIndex(name string, options ...fieldIndexOption) Definition {
	fi := fieldIndexDef{name: name}
	for _, opt := range options {
		opt.apply(&fi)
	}
	return fi
}

type fieldIndexOption interface {
	apply(fi *fieldIndexDef)
}

// SkipZeros is an option to FieldIndex that makes the index exclude entities
// with zero values of the field. If the field is a pointer, zero values of the
// pointed-to type are excluded.
var SkipZeros skipZeros

type skipZeros struct{}

func (skipZeros) apply(fi *fieldIndexDef) {
	fi.skipZeros = true
}

// IgnoreCase is an option to FieldIndex that makes the index case-insensitive
// (per Unicode spec). The field must be a string, a named type based on a
// string, or a pointer to one of those. Values that only differ in case are
// considered equal by a case-insensitive index. In a UniqueIndex, they
// conflict. Lookup is case-insensitive.
var IgnoreCase ignoreCase

type ignoreCase struct{}

func (ignoreCase) apply(fi *fieldIndexDef) {
	fi.ignoreCase = true
}

func (fid fieldIndexDef) Name() string {
	return fid.name
}

func (fid fieldIndexDef) Args() int {
	return 1
}

func (fid fieldIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	field, ok := s.Field(fid.name)
	if !ok {
		panic(fmt.Errorf("field %s.%s not found", s.Type, fid.name))
	}
	t := field.Type
	switch t.Kind() {
	case reflect.Slice:
		elemT := t.Elem()
		if elemT.Kind() != reflect.String {
			panic(fmt.Errorf("field %s.%s should be a slice of string-based elements", s.Type, fid.name))
		}
		return &memdb.IndexSchema{
			Name: fid.name,
			Indexer: &stringSliceFieldIndexer{
				multiIndexer: multiIndexer{
					def:   fid,
					t:     elemT,
					index: field.Index,
				},
			},
		}
	case reflect.Map:
		keyT := t.Key()
		if keyT.Kind() != reflect.String {
			panic(fmt.Errorf("field %s.%s should be a map with string-based keys", s.Type, fid.name))
		}
		return &memdb.IndexSchema{
			Name: fid.name,
			Indexer: &stringMapFieldIndexer{
				multiIndexer: multiIndexer{
					def:   fid,
					t:     keyT,
					index: field.Index,
				},
			},
		}
	default:
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		keyFn := keyFnForType(t)
		if keyFn == nil {
			panic(fmt.Errorf("field %s.%s has unsupported type %s", s.Type, fid.name, field.Type))
		}
		if fid.ignoreCase {
			if t.Kind() != reflect.String {
				panic(fmt.Errorf("field %s.%s must be string-based for case-insensitive indexing", s.Type, fid.name))
			}
			keyFn = func(v reflect.Value) ([]byte, bool) {
				s := v.String()
				return []byte(strings.ToLower(s) + "\x00"), s != ""
			}
		}
		fieldIndexer := fieldIndexer{def: fid, t: t, index: field.Index, keyFn: keyFn}
		schema := memdb.IndexSchema{
			Name:         fid.Name(),
			AllowMissing: fid.skipZeros,
			Indexer:      fieldIndexer,
		}
		if field.Type.Kind() == reflect.Ptr {
			schema.AllowMissing = true
			schema.Indexer = ptrFieldIndexer{fieldIndexer}
		}
		return &schema
	}
}

type fieldIndexer struct {
	def   fieldIndexDef
	t     reflect.Type
	index []int
	keyFn keyFn
}

func (fi fieldIndexer) FromArgs(args ...any) ([]byte, error) {
	v := reflect.ValueOf(args[0])
	if v.Type() != fi.t {
		return nil, fmt.Errorf("index %s expects %s value", fi.def.Name(), fi.t)
	}
	k, _ := fi.keyFn(v)
	return k, nil
}

func (fi fieldIndexer) FromObject(obj any) (bool, []byte, error) {
	v := reflect.ValueOf(obj).FieldByIndex(fi.index)
	k, ok := fi.keyFn(v)
	if !ok && fi.def.skipZeros {
		return false, nil, nil
	}
	return true, k, nil
}

type ptrFieldIndexer struct {
	fieldIndexer
}

func (pfi ptrFieldIndexer) FromObject(obj any) (bool, []byte, error) {
	v := reflect.ValueOf(obj).FieldByIndex(pfi.index)
	if v.IsNil() {
		return false, nil, nil
	}
	k, ok := pfi.keyFn(v.Elem())
	if !ok && pfi.def.skipZeros {
		return false, nil, nil
	}
	return true, k, nil
}
