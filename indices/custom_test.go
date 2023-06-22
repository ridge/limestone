package indices

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestCustomIndex(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     BarID
		Number    int
	}

	def := CustomIndex("BarNumber", func(foo Foo) (BarID, int, bool) { return foo.BarID, foo.Number, foo.BarID != "" })
	require.Equal(t, "BarNumber", def.Name())
	require.Equal(t, 2, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarNumber", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	_, err := schema.Indexer.FromArgs("!", 42)
	require.Error(t, err)
	b, err := schema.Indexer.FromArgs(BarID("!"), 42)
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a}, b)

	prefix := schema.Indexer.(memdb.PrefixIndexer)
	_, err = prefix.PrefixFromArgs("!")
	require.Error(t, err)
	b, err = prefix.PrefixFromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{})
	testSingleFromObjectValuesNotFound(t, single, Foo{Number: 42})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "!"}, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "!", Number: 42}, []byte{0x21, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a})
}

func TestCustomIndexInterface(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta    `limestone:"name=foo"`
		ID           FooID `limestone:"identity"`
		fmt.Stringer `limestone:"-"`
	}

	def := CustomIndex("Stringer", func(s fmt.Stringer) (int, string, bool) { return len(s.String()), s.String(), s.String() != "" })
	require.Equal(t, "Stringer", def.Name())
	require.Equal(t, 2, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Stringer", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(1, "!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00}, b)

	prefix := schema.Indexer.(memdb.PrefixIndexer)
	b, err = prefix.PrefixFromArgs(1)
	require.NoError(t, err)
	require.Equal(t, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, b)

	var str strings.Builder
	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{Stringer: &str})
	str.WriteString("!")
	testSingleFromObjectValuesFound(t, single, Foo{Stringer: &str}, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00})
}
