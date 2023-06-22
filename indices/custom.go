package indices

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type customIndexDef struct {
	name  string
	fn    reflect.Value
	types []reflect.Type
	keyFn []keyFn
}

// CustomIndex specifies an index implemented by a custom function. The function
// takes the entity by value as its only argument, and returns zero or more typed
// values, plus a bool value (must be last). The bool value indicates whether or
// not the entity must be present in the index. It will be possible to search
// for the entities by passing values of the same types (except the last bool
// one) in the same order to snapshot.Search.
//
// The name argument to CustomIndex is to identify this index uniquely. Give
// different names to indices using different functions.
//
// Example:
//
//	// indexDeficit includes only clusters that are over budget,
//	// sorted by how much over budget they are.
//	var indexDeficit = CustomIndex("deficit",
//	    func(c cluster) (units.NanoUSDPerHour, bool) {
//	        if c.Budget == nil {
//	            return 0, false
//	        }
//	        // the difference will be negative, sorted in ascending order
//	        return c.Budget - c.SpendingRate, c.SpendingRate > c.Budget
//	    })
//	var kindCluster = KindOf(cluster{}, indexDeficit)
//
// To enumerate all clusters that are over budget, from biggest deficit to
// smallest:
//
//	iter := snapshot.Search(kindCluster, indexDeficit)
//
// To find the clusters with a particular deficit value:
//
//	iter := snapshot.Search(kindCluster, indexDeficit, units.NanoUSDPerHour(-1000))
func CustomIndex(name string, fn any) Definition {
	t := reflect.TypeOf(fn)
	if t.Kind() != reflect.Func {
		panic(fmt.Errorf("index %s: argument is not a function", name))
	}
	if t.NumIn() != 1 {
		panic(fmt.Errorf("index %s: function must take exactly one argument", name))
	}
	if t.NumOut() < 1 {
		panic(fmt.Errorf("index %s: function must return at least one value", name))
	}
	if t.Out(t.NumOut()-1) != reflect.TypeOf(false) {
		panic(fmt.Errorf("index %s: function must return bool as the last value", name))
	}
	cid := customIndexDef{
		name:  name,
		fn:    reflect.ValueOf(fn),
		types: make([]reflect.Type, 0, t.NumOut()-1),
		keyFn: make([]keyFn, 0, t.NumOut()-1),
	}
	for i := 0; i < t.NumOut()-1; i++ {
		rvt := t.Out(i)
		keyFn := keyFnForType(rvt)
		if keyFn == nil {
			panic(fmt.Errorf("index %s: return value #%d has unsupported type %s", name, i, t))
		}
		cid.types = append(cid.types, rvt)
		cid.keyFn = append(cid.keyFn, keyFn)
	}
	return cid
}

func (cid customIndexDef) Name() string {
	return cid.name
}

func (cid customIndexDef) Args() int {
	return len(cid.types)
}

func (cid customIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	if !s.Type.AssignableTo(cid.fn.Type().In(0)) {
		panic(fmt.Errorf("index %s: function argument type must admit %s", cid.Name(), s.Type))
	}
	return &memdb.IndexSchema{
		Name:         cid.Name(),
		AllowMissing: true,
		Indexer:      customIndexer{def: cid},
	}
}

type customIndexer struct {
	def customIndexDef
}

func (ci customIndexer) FromArgs(args ...any) ([]byte, error) {
	return ci.PrefixFromArgs(args...)
}

func (ci customIndexer) PrefixFromArgs(args ...any) ([]byte, error) {
	size := 0
	fragments := make([][]byte, 0, len(args))
	for i, arg := range args {
		v := reflect.ValueOf(arg)
		if v.Type() != ci.def.types[i] {
			return nil, fmt.Errorf("index %s: argument #%d must have type %s", ci.def.Name(), i, ci.def.types[i])
		}
		b, _ := ci.def.keyFn[i](v)
		size += len(b)
		fragments = append(fragments, b)
	}
	whole := make([]byte, 0, size)
	for _, fragment := range fragments {
		whole = append(whole, fragment...)
	}
	return whole, nil
}

func (ci customIndexer) FromObject(obj any) (bool, []byte, error) {
	res := ci.def.fn.Call([]reflect.Value{reflect.ValueOf(obj)})
	if !res[len(res)-1].Bool() {
		return false, nil, nil
	}
	size := 0
	fragments := make([][]byte, 0, len(res)-1)
	for i, v := range res[0 : len(res)-1] {
		b, _ := ci.def.keyFn[i](v)
		size += len(b)
		fragments = append(fragments, b)
	}
	whole := make([]byte, 0, size)
	for _, fragment := range fragments {
		whole = append(whole, fragment...)
	}
	return true, whole, nil
}
