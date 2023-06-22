package typeddb

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/meta"
)

// Kind describes a particular type of objects handled by the framework.
// All fields are read-only.
type Kind struct {
	meta.Struct
	Indices     map[string]indices.Definition
	identity    indices.Definition
	indexSchema map[string]*memdb.IndexSchema
}

// KindOf creates a Kind for a given object example and index definitions.
// See package documentation for lib/limestone/indices.
func KindOf(example any, indexDefs ...indices.Definition) *Kind {
	t := reflect.TypeOf(example)
	metaStruct := meta.Survey(t)
	identity := indices.IdentityIndex()
	kind := Kind{
		Struct:   metaStruct,
		Indices:  map[string]indices.Definition{},
		identity: identity,
		indexSchema: map[string]*memdb.IndexSchema{
			identity.Name(): identity.Index(metaStruct),
		},
	}

	for _, indexDef := range indexDefs {
		name := indexDef.Name()
		if kind.indexSchema[name] != nil {
			panic(fmt.Sprintf("duplicate index name on %s: %s", metaStruct, name))
		}
		kind.Indices[name] = indexDef
		kind.indexSchema[name] = indexDef.Index(metaStruct)
	}

	return &kind
}
