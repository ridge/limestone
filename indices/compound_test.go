package indices

import (
	"reflect"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestCompoundIndex(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     BarID
		Number    int
	}

	def := CompoundIndex(FieldIndex("BarID", SkipZeros), FieldIndex("Number"))
	require.Equal(t, "BarID,Number", def.Name())
	require.Equal(t, 2, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarID,Number", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"), 42)
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a}, b)

	prefix := schema.Indexer.(memdb.PrefixIndexer)
	b, err = prefix.PrefixFromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "!"}, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	testSingleFromObjectValuesNotFound(t, single, Foo{Number: 42})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "!", Number: 42}, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a})
}
