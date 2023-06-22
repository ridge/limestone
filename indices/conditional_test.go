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

func TestConditionalIndexWhereTrue(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Name      string
		Active    bool
	}

	def := IndexIfNonzero("Active", FieldIndex("Name"))
	require.Equal(t, "Name[Active]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name[Active]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{Name: "!"})
	testSingleFromObjectValuesFound(t, single, Foo{Name: "!", Active: true}, []byte{0x21, 0x00})
}

func TestConditionalIndexWhereFalse(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Name      string
		Deleted   bool
	}

	def := IndexIfZero("Deleted", FieldIndex("Name"))
	require.Equal(t, "Name[!Deleted]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name[!Deleted]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesFound(t, single, Foo{Name: "!"}, []byte{0x21, 0x00})
	testSingleFromObjectValuesNotFound(t, single, Foo{Name: "!", Deleted: true})
}

func TestConditionalIndexCustom(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Name      string
	}

	def := IndexIf("Short", func(foo Foo) bool { return len(foo.Name) < 2 }, FieldIndex("Name"))
	require.Equal(t, "Name[Short]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name[Short]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesFound(t, single, Foo{Name: "!"}, []byte{0x21, 0x00})
	testSingleFromObjectValuesNotFound(t, single, Foo{Name: "!!"})
}

func TestConditionalIndexCustomInterface(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta    `limestone:"name=foo"`
		ID           FooID `limestone:"identity"`
		Name         string
		fmt.Stringer `limestone:"-"`
	}

	def := IndexIf("Short", func(s fmt.Stringer) bool { return len(s.String()) < 2 }, FieldIndex("Name"))
	require.Equal(t, "Name[Short]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name[Short]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	var str strings.Builder
	str.WriteString("*")
	testSingleFromObjectValuesFound(t, single, Foo{Name: "!", Stringer: &str}, []byte{0x21, 0x00})
	str.WriteString("**")
	testSingleFromObjectValuesNotFound(t, single, Foo{Name: "!", Stringer: &str})
}

func TestConditionalIndexCompound(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		A, B      string
		Active    bool
	}

	def := IndexIfNonzero("Active", CompoundIndex(FieldIndex("A"), FieldIndex("B")))
	require.Equal(t, "A,B[Active]", def.Name())
	require.Equal(t, 2, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "A,B[Active]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("!", "1")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00, 0x31, 0x00}, b)

	prefix := schema.Indexer.(memdb.PrefixIndexer)
	b, err = prefix.PrefixFromArgs("!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.SingleIndexer)

	testSingleFromObjectValuesNotFound(t, multi, Foo{A: "!", B: "1"})
	testSingleFromObjectValuesFound(t, multi, Foo{A: "!", B: "1", Active: true}, []byte{0x21, 0x00, 0x31, 0x00})
}

func TestConditionalMultiIndexWhereTrue(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
		Active    bool
	}

	def := IndexIfNonzero("Active", FieldIndex("BarIDs"))
	require.Equal(t, "BarIDs[Active]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs[Active]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesNotFound(t, multi, Foo{BarIDs: []BarID{"!"}})
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"!"}, Active: true}, [][]byte{{0x21, 0x00}})
}

func TestConditionalMultiIndexWhereFalse(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
		Deleted   bool
	}

	def := IndexIfZero("Deleted", FieldIndex("BarIDs"))
	require.Equal(t, "BarIDs[!Deleted]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs[!Deleted]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"!"}}, [][]byte{{0x21, 0x00}})
	testMultiFromObjectValuesNotFound(t, multi, Foo{BarIDs: []BarID{"!"}, Deleted: true})
}

func TestConditionalMultiIndexCustom(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
	}

	def := IndexIf("Short", func(foo Foo) bool { return len(foo.BarIDs) < 2 }, FieldIndex("BarIDs"))
	require.Equal(t, "BarIDs[Short]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs[Short]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"!"}}, [][]byte{{0x21, 0x00}})
	testMultiFromObjectValuesNotFound(t, multi, Foo{BarIDs: []BarID{"!!", "zz"}})
}

func TestConditionalMultiIndexCustomInterface(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta    `limestone:"name=foo"`
		ID           FooID `limestone:"identity"`
		BarIDs       []BarID
		fmt.Stringer `limestone:"-"`
	}

	def := IndexIf("Short", func(s fmt.Stringer) bool { return len(s.String()) < 2 }, FieldIndex("BarIDs"))
	require.Equal(t, "BarIDs[Short]", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs[Short]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	var str strings.Builder
	str.WriteString("*")
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"!"}, Stringer: &str}, [][]byte{{0x21, 0x00}})
	str.WriteString("**")
	testMultiFromObjectValuesNotFound(t, multi, Foo{BarIDs: []BarID{"!"}, Stringer: &str})
}

func TestConditionalMultiIndexCompound(t *testing.T) {
	type FooID string
	type BarID string
	type BazID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
		BazIDs    []BazID
		Active    bool
	}

	def := IndexIfNonzero("Active", CompoundIndex(FieldIndex("BarIDs"), FieldIndex("BazIDs")))
	require.Equal(t, "BarIDs,BazIDs[Active]", def.Name())
	require.Equal(t, 2, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs,BazIDs[Active]", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID("!"), BazID("1"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00, 0x31, 0x00}, b)

	prefix := schema.Indexer.(memdb.PrefixIndexer)
	b, err = prefix.PrefixFromArgs(BarID("!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x00}, b)
	//
	// single := schema.Indexer.(memdb.SingleIndexer)
	// ok, b, err := single.FromObject(Foo{A: "!", B: "1"})
	// require.NoError(t, err)
	// require.False(t, ok)
	// require.Nil(t, b)
	// ok, b, err = single.FromObject(Foo{A: "!", B: "1", Active: true})
	// require.NoError(t, err)
	// require.True(t, ok)
	// require.Equal(t, []byte{0x21, 0x00, 0x31, 0x00}, b)
}
