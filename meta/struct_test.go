package meta

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestField(t *testing.T) {
	require.Equal(t, "Foo", Field{GoName: "Foo"}.String())
	require.Equal(t, "Foo (foo)", Field{GoName: "Foo", DBName: "foo"}.String())
}

func TestStruct(t *testing.T) {
	type FooID string
	type Foo struct{ ID FooID }
	s := Struct{
		DBName: "foo",
		Type:   reflect.TypeOf(Foo{}),
		Fields: []Field{
			{
				GoName: "ID",
				DBName: "ID",
				Index:  []int{0},
				Type:   reflect.TypeOf(FooID("")),
			},
		},
		identity: 0,
	}
	require.Equal(t, "meta.Foo (foo)", s.String())
	require.Equal(t, Field{
		GoName: "ID",
		DBName: "ID",
		Index:  []int{0},
		Type:   reflect.TypeOf(FooID("")),
	}, s.Identity())
	field, ok := s.Field("ID")
	require.True(t, ok)
	require.Equal(t, s.Identity(), field)
	_, ok = s.Field("id")
	require.False(t, ok)
}
