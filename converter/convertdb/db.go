package convertdb

import (
	"github.com/ridge/limestone/meta"
	"github.com/ridge/limestone/typeddb"
)

// DB is an in-memory database for entities under upgrade
type DB struct {
	tdb   *typeddb.TypedDB
	kinds map[string]*Kind
}

// New creates a new DB
func New(kinds KindList) *DB {
	kl := make([]*typeddb.Kind, 0, len(kinds))
	byName := map[string]*Kind{}
	for _, kind := range kinds {
		kl = append(kl, kind.Kind)
		byName[kind.DBName] = kind
	}
	return &DB{
		tdb:   typeddb.New(kl),
		kinds: byName,
	}
}

// Meta is a type for dummy fields bearing tags for the containing structure
type Meta = meta.Meta
