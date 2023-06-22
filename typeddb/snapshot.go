package typeddb

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/indices"
	"github.com/ridge/must/v2"
	"time"
)

// Iterator is an iterator over the results of a query. Every call fills in
// another object into ptr argument. Returns false when the end of dataset is
// reached (data at ptr won't be modified in this call).
//
// Iterators accept nil instead of a pointer. When the argument is nil, an
// iterator discards one item. The calling code can use this to skip sequence
// items.
//
// Do not use a signle iterator concurrently.
type Iterator = func(ptr any) bool

// Snapshot is a read-only view of the local database at point in time it was created.
// Safe for concurrent use.
type Snapshot interface {
	// Time returns the time when the snapshot was taken.
	Time() time.Time
	// Get copies a value of an object of a kind determined by a type of ptr
	// with a specified id. Returns false if the entity does not exist.
	//
	// id should be a string or a type based on string.
	Get(id any, ptr any) bool
	// All returns an iterator over all the entities of a specific kind.
	All(kind *Kind) Iterator
	// Search returns an iterator over all entities using an index.
	Search(kind *Kind, index indices.Definition, args ...any) Iterator
}

type snapshot struct {
	tdb *TypedDB
	txn *memdb.Txn
	ts  time.Time
}

func (s snapshot) Time() time.Time {
	return s.ts
}

func get(txn *memdb.Txn, eid EID) any {
	return must.OK1(txn.First(eid.Kind.DBName, "id", eid.ID))
}

// Get returns an object from the database, using passed ID and filling passed
// ptr. Object kind is deduced from type of the ptr. Returns whether the object
// was found.
func (s snapshot) Get(id any, ptr any) bool {
	rid := reflect.ValueOf(id)
	if rid.Kind() != reflect.String {
		panic("id must be a string")
	}
	kind := s.tdb.kindOfPtr(ptr)

	res := get(s.txn, EID{kind, rid.String()})
	if res == nil {
		return false
	}

	reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(res))
	return true
}

// All returns an iterator over all objects of a given kind.
func (s snapshot) All(kind *Kind) Iterator {
	return s.Search(kind, kind.identity)
}

// Search returns an iterator over all objects of a given kind that are found by
// search. Search is performed using an index; args are matched to the index
// values (see limestone/indices library).
func (s snapshot) Search(kind *Kind, index indices.Definition, args ...any) Iterator {
	name := index.Name()
	if len(args) > index.Args() {
		panic(fmt.Errorf("index %s expects up to %d arguments", name, index.Args()))
	}
	schema := kind.indexSchema[name]
	if schema == nil {
		panic(fmt.Errorf("index %s for kind %s not found", name, kind))
	}
	if len(args) > 0 && len(args) < index.Args() {
		if _, ok := schema.Indexer.(memdb.PrefixIndexer); !ok {
			panic(fmt.Errorf("index %s expects exactly %d arguments", name, index.Args()))
		}
		name += "_prefix" // magic suffix recognized by memdb to enable prefix search
	}
	iter := must.OK1(s.txn.Get(kind.DBName, name, args...))

	return func(ptr any) bool {
		res := iter.Next()
		if res == nil {
			return false
		}

		if ptr != nil {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(res))
		}
		return true
	}
}

// MustGet returns an object from the database like Snapshot.Get, panic()ing if
// the object is not found
func MustGet(s Snapshot, id any, ptr any) {
	if !s.Get(id, ptr) {
		panic(fmt.Errorf("%T(%q) not found", id, id))
	}
}
