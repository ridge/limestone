package indices

import (
	"reflect"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestIdentityIndex(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
	}

	def := IdentityIndex()
	require.Equal(t, "id", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "id", schema.Name)
	require.False(t, schema.AllowMissing)
	require.True(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)
	b, err = schema.Indexer.FromArgs(FooID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesFound(t, single, Foo{ID: "!"}, []byte{0x21, 0x00})
}
