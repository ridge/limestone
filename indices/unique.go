package indices

import (
	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type uniqueIndexDef struct {
	def Definition
}

// UniquifyIndex makes an index unique
func UniquifyIndex(def Definition) Definition {
	return uniqueIndexDef{def: def}
}

func (uid uniqueIndexDef) Name() string {
	return uid.def.Name()
}

func (uid uniqueIndexDef) Args() int {
	return uid.def.Args()
}

func (uid uniqueIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	schema := uid.def.Index(s)
	return &memdb.IndexSchema{
		Name:         schema.Name,
		AllowMissing: schema.AllowMissing,
		Unique:       true,
		Indexer:      schema.Indexer,
	}
}
