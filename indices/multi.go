package indices

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const null = "\x00"

type multiIndexer struct {
	def   fieldIndexDef
	t     reflect.Type
	index []int
}

func (i multiIndexer) field(obj any) reflect.Value {
	return reflect.ValueOf(obj).FieldByIndex(i.index)
}

func (i multiIndexer) appendVal(vals *[][]byte, val string) {
	if val == "" && i.def.skipZeros {
		return
	}
	if i.def.ignoreCase {
		val = strings.ToLower(val)
	}

	val += null // Add the null character as a terminator
	*vals = append(*vals, []byte(val))
}

func (i multiIndexer) FromArgs(args ...any) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide only a single argument")
	}
	if reflect.TypeOf(args[0]) != i.t {
		return nil, fmt.Errorf("argument must be a %s: %#v", i.t, args[0])
	}
	arg := reflect.ValueOf(args[0]).String()
	if i.def.ignoreCase {
		arg = strings.ToLower(arg)
	}
	// Add the null character as a terminator
	arg += null
	return []byte(arg), nil
}

// stringSliceFieldIndexer builds an index from a field on an object that is a slice of string-based values.
// Each value within the slice can be used for lookup.
type stringSliceFieldIndexer struct {
	multiIndexer
}

func (s *stringSliceFieldIndexer) FromObject(obj any) (bool, [][]byte, error) {
	fv := s.field(obj)
	vals := make([][]byte, 0, fv.Len())
	for i, length := 0, fv.Len(); i < length; i++ {
		s.appendVal(&vals, fv.Index(i).String())
	}
	return true, vals, nil
}

// stringMapFieldIndexer builds an index from a field on an object that is a map with string-based keys and
// values of any type.
// Each key within the map can be used for lookup.
type stringMapFieldIndexer struct {
	multiIndexer
}

func (s *stringMapFieldIndexer) FromObject(obj any) (bool, [][]byte, error) {
	fv := s.field(obj)
	vals := make([][]byte, 0, fv.Len())
	for _, key := range fv.MapKeys() {
		s.appendVal(&vals, key.String())
	}
	sort.Slice(vals, func(i, j int) bool { return bytes.Compare(vals[i], vals[j]) < 0 })
	return true, vals, nil
}
