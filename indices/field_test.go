package indices

import (
	"reflect"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func testSingleFromObjectValuesFound(t *testing.T, single memdb.SingleIndexer, obj any, expectedBytes []byte) {
	ok, bytes, err := single.FromObject(obj)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expectedBytes, bytes)
}

func testSingleFromObjectValuesNotFound(t *testing.T, single memdb.SingleIndexer, obj any) {
	ok, bytes, err := single.FromObject(obj)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, bytes)
}

func testMultiFromObjectValuesFound(t *testing.T, multi memdb.MultiIndexer, obj any, expectedBytes [][]byte) {
	ok, bytes, err := multi.FromObject(obj)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expectedBytes, bytes)
}

func testMultiFromObjectValuesNotFound(t *testing.T, multi memdb.MultiIndexer, obj any) {
	ok, bytes, err := multi.FromObject(obj)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, bytes)
}

func TestFieldIndex(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     BarID
	}

	def := FieldIndex("BarID")
	require.Equal(t, "BarID", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarID", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesFound(t, single, Foo{}, []byte{0x00})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "aA!"}, []byte{0x61, 0x41, 0x21, 0x00})
}

func TestFieldIndexSkipZeros(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     BarID
	}

	def := FieldIndex("BarID", SkipZeros)
	require.Equal(t, "BarID", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarID", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{})
	testSingleFromObjectValuesFound(t, single, Foo{BarID: "aA!"}, []byte{0x61, 0x41, 0x21, 0x00})
}

func TestFieldIndexPtr(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     *BarID
	}

	def := FieldIndex("BarID")
	require.Equal(t, "BarID", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarID", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{})
	barID0 := BarID("")
	testSingleFromObjectValuesFound(t, single, Foo{BarID: &barID0}, []byte{0x00})
	barID1 := BarID("aA!")
	testSingleFromObjectValuesFound(t, single, Foo{BarID: &barID1}, []byte{0x61, 0x41, 0x21, 0x00})
}

func TestFieldIndexSkipZerosPtr(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarID     *BarID
	}

	def := FieldIndex("BarID", SkipZeros)
	require.Equal(t, "BarID", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarID", schema.Name)
	require.True(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesNotFound(t, single, Foo{})
	barID0 := BarID("")
	testSingleFromObjectValuesNotFound(t, single, Foo{BarID: &barID0})
	barID1 := BarID("aA!")
	testSingleFromObjectValuesFound(t, single, Foo{BarID: &barID1}, []byte{0x61, 0x41, 0x21, 0x00})
}

func TestFieldIndexIgnoreCase(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Name      string
	}

	def := FieldIndex("Name", IgnoreCase)
	require.Equal(t, "Name", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x61, 0x21, 0x00}, b)

	single := schema.Indexer.(memdb.SingleIndexer)
	testSingleFromObjectValuesFound(t, single, Foo{}, []byte{0x00})
	testSingleFromObjectValuesFound(t, single, Foo{Name: "aA!"}, []byte{0x61, 0x61, 0x21, 0x00})
}

func TestFieldIndexStringSlice(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Strings   []string
	}

	def := FieldIndex("Strings")
	require.Equal(t, "Strings", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Strings", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{Strings: []string{"aA!"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexStringSliceSkipZeros(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Strings   []string
	}

	def := FieldIndex("Strings", SkipZeros)
	require.Equal(t, "Strings", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Strings", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{Strings: []string{"aA!"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexStringSliceIgnoreCase(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Strings   []string
	}

	def := FieldIndex("Strings", IgnoreCase)
	require.Equal(t, "Strings", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Strings", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x61, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{Strings: []string{"aA!"}}, [][]byte{{0x61, 0x61, 0x21, 0x00}})
}

func TestFieldIndexTypedStringSlice(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
	}

	def := FieldIndex("BarIDs")
	require.Equal(t, "BarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"aA!"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexTypedStringSliceSkipZeros(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
	}

	def := FieldIndex("BarIDs", SkipZeros)
	require.Equal(t, "BarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"aA!"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexTypedStringSliceIgnoreCase(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		BarIDs    []BarID
	}

	def := FieldIndex("BarIDs", IgnoreCase)
	require.Equal(t, "BarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "BarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x61, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{BarIDs: []BarID{"aA!"}}, [][]byte{{0x61, 0x61, 0x21, 0x00}})
}

func TestFieldIndexStringMap(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		StringMap map[string]string
	}

	def := FieldIndex("StringMap")
	require.Equal(t, "StringMap", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "StringMap", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{StringMap: map[string]string{"aA!": "boo"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexStringMapSkipZeros(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		StringMap map[string]string
	}

	def := FieldIndex("StringMap", SkipZeros)
	require.Equal(t, "StringMap", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "StringMap", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{StringMap: map[string]string{"aA!": "boo"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexStringMapIgnoreCase(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		StringMap map[string]string
	}

	def := FieldIndex("StringMap", IgnoreCase)
	require.Equal(t, "StringMap", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "StringMap", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs("aA!")
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x61, 0x21, 0x00}, b)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{StringMap: map[string]string{"aA!": "boo"}}, [][]byte{{0x61, 0x61, 0x21, 0x00}})
}

func TestFieldIndexTypedStringMap(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		ByBarIDs  map[BarID]any
	}

	def := FieldIndex("ByBarIDs")
	require.Equal(t, "ByBarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "ByBarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{ByBarIDs: map[BarID]any{"aA!": "boo"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexTypedStringMapSkipZeros(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		ByBarIDs  map[BarID]any
	}

	def := FieldIndex("ByBarIDs", SkipZeros)
	require.Equal(t, "ByBarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "ByBarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x41, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{ByBarIDs: map[BarID]any{"aA!": "boo"}}, [][]byte{{0x61, 0x41, 0x21, 0x00}})
}

func TestFieldIndexTypedStringMapIgnoreCase(t *testing.T) {
	type FooID string
	type BarID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		ByBarIDs  map[BarID]any
	}

	def := FieldIndex("ByBarIDs", IgnoreCase)
	require.Equal(t, "ByBarIDs", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "ByBarIDs", schema.Name)
	require.False(t, schema.AllowMissing)
	require.False(t, schema.Unique)

	b, err := schema.Indexer.FromArgs(BarID(""))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)
	b, err = schema.Indexer.FromArgs(BarID("aA!"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x61, 0x61, 0x21, 0x00}, b)
	_, err = schema.Indexer.FromArgs("aA!")
	require.Error(t, err)

	multi := schema.Indexer.(memdb.MultiIndexer)
	testMultiFromObjectValuesFound(t, multi, Foo{}, [][]byte{})
	testMultiFromObjectValuesFound(t, multi, Foo{ByBarIDs: map[BarID]any{"aA!": "boo"}}, [][]byte{{0x61, 0x61, 0x21, 0x00}})
}
