package typeddb

import (
	"reflect"
	"testing"

	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestKind(t *testing.T) {
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        string `limestone:"identity"`
	}
	kind := KindOf(Foo{})
	require.Equal(t, meta.Struct{
		DBName: "foo",
		Type:   reflect.TypeOf(Foo{}),
		Fields: []meta.Field{
			{
				GoName:   "ID",
				DBName:   "ID",
				Index:    []int{1},
				Type:     reflect.TypeOf(""),
				Const:    true,
				Required: true,
			},
		},
	}, kind.Struct)
	require.PanicsWithValue(t, "duplicate index name on typeddb.Foo (foo): ID", func() {
		_ = KindOf(Foo{},
			indices.FieldIndex("ID"),
			indices.FieldIndex("ID"),
		)
	})
}
