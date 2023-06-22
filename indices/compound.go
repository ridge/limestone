package indices

import (
	"strings"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type compoundIndexDef struct {
	args int
	defs []Definition
}

// CompoundIndex specifies a compound index built from several simpler indices.
// Only produces values for entities for which all constituent indices produce
// values.
//
// Example:
//
//	var indexFleetIDGroup = limestone.CompoundIndex(
//	    limestone.FieldIndex("FleetID"),
//	    limestone.FieldIndex("Group"),
//	)
//	var kindInstance = limestone.KindOf(instance{}, indexFleetIDGroup)
//
// To find instances with the particular FleetID and Group:
//
//	iter := snapshot.Search(kindInstance, indexFleetIDGroup, fleet.ID, groupName)
//
// To find instances with the particular FleetID, sorted by Group:
//
//	iter := snapshot.Search(kindInstance, indexFleetIDGroup, fleet.ID)
//
// To enumerate all instances, sorted first by FleetID, then by Group:
//
//	iter := snapshot.Search(kindInstance, indexFleetIDGroup)
func CompoundIndex(defs ...Definition) Definition {
	args := 0
	for _, def := range defs {
		args += def.Args()
	}
	return compoundIndexDef{
		args: args,
		defs: defs,
	}
}

func (cid compoundIndexDef) Name() string {
	names := make([]string, 0, len(cid.defs))
	for _, def := range cid.defs {
		names = append(names, def.Name())
	}
	return strings.Join(names, ",")
}

func (cid compoundIndexDef) Args() int {
	return cid.args
}

func (cid compoundIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	indexers := make([]memdb.Indexer, 0, len(cid.defs))
	allowMissing := false
	for _, def := range cid.defs {
		schema := def.Index(s)
		indexers = append(indexers, schema.Indexer)
		if schema.AllowMissing {
			allowMissing = true
		}
	}
	return &memdb.IndexSchema{
		Name:         cid.Name(),
		AllowMissing: allowMissing,
		Indexer: compoundIndexer{
			def:      cid,
			indexers: indexers,
		},
	}
}

type compoundIndexer struct {
	def      compoundIndexDef
	indexers []memdb.Indexer
}

func (ci compoundIndexer) FromArgs(args ...any) ([]byte, error) {
	return ci.PrefixFromArgs(args...)
}

func (ci compoundIndexer) PrefixFromArgs(args ...any) ([]byte, error) {
	offset := 0
	size := 0
	fragments := make([][]byte, 0, len(ci.indexers))
	for i, index := range ci.def.defs {
		if offset >= len(args) { // end of prefix
			break
		}
		a := index.Args()
		var b []byte
		var err error
		if offset+a > len(args) { // nested prefix search
			b, err = ci.indexers[i].(memdb.PrefixIndexer).PrefixFromArgs(args[offset:]...)
		} else {
			b, err = ci.indexers[i].FromArgs(args[offset : offset+a]...)
		}
		if err != nil {
			return nil, err
		}
		offset += a
		size += len(b)
		fragments = append(fragments, b)
	}
	whole := make([]byte, 0, size)
	for _, fragment := range fragments {
		whole = append(whole, fragment...)
	}
	return whole, nil
}

func (ci compoundIndexer) FromObject(obj any) (bool, []byte, error) {
	size := 0
	fragments := make([][]byte, 0, len(ci.indexers))
	for _, indexer := range ci.indexers {
		ok, b, err := indexer.(memdb.SingleIndexer).FromObject(obj)
		if err != nil {
			return false, nil, err
		}
		if !ok {
			return false, nil, nil
		}
		size += len(b)
		fragments = append(fragments, b)
	}
	whole := make([]byte, 0, size)
	for _, fragment := range fragments {
		whole = append(whole, fragment...)
	}
	return true, whole, nil
}
