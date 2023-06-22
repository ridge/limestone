package indices

import (
	"reflect"
	"testing"

	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestUniquifyIndex(t *testing.T) {
	type FooID string
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        FooID `limestone:"identity"`
		Name      string
	}

	def := UniquifyIndex(FieldIndex("Name"))
	require.Equal(t, "Name", def.Name())
	require.Equal(t, 1, def.Args())

	schema := def.Index(meta.Survey(reflect.TypeOf(Foo{})))
	require.Equal(t, "Name", schema.Name)
	require.False(t, schema.AllowMissing)
	require.True(t, schema.Unique)
}
