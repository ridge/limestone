package indices

import (
	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

// Definition is a blueprint for creating a memdb indexer. This intermediate step
// is needed because such blueprints are created before the Kind they are
// intended for.
type Definition interface {
	Name() string                         // stable unique name
	Args() int                            // number of expected Search arguments
	Index(meta.Struct) *memdb.IndexSchema // create the indexer
}
