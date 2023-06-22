package indices

import (
	"errors"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
)

type identityIndexDef struct{}

// IdentityIndex specifies a unique index on the identity field
func IdentityIndex() Definition {
	return identityIndexDef{}
}

func (identityIndexDef) Name() string {
	return "id" // this is the constant primary index name expected by memdb
}

func (identityIndexDef) Args() int {
	return 1
}

func (identityIndexDef) Index(s meta.Struct) *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: identityIndexer{index: s.Identity().Index},
	}
}

type identityIndexer struct {
	index []int
}

func (ii identityIndexer) FromArgs(args ...any) ([]byte, error) {
	v := reflect.ValueOf(args[0])
	if v.Kind() != reflect.String {
		return nil, errors.New("primary key index expects a string")
	}
	return []byte(v.String() + "\x00"), nil
}

func (ii identityIndexer) FromObject(obj any) (bool, []byte, error) {
	v := reflect.ValueOf(obj).FieldByIndex(ii.index)
	return true, []byte(v.String() + "\x00"), nil
}
