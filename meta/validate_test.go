package meta

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRequired(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo"`
		ID     string `limestone:"identity"`
		Field1 int    `limestone:"required"`
		Field2 int
		List1  []int `limestone:"required"`
		List2  []int
		Map1   map[string]int `limestone:"required"`
		Map2   map[string]int
	}
	s := Survey(reflect.TypeOf(Foo{}))
	require.PanicsWithValue(t, "expected struct type meta.Foo",
		func() { _ = s.ValidateRequired(0) })
	require.EqualError(t, s.ValidateRequired(Foo{}),
		"meta.Foo (foo) validation failed: missing required field ID")
	require.EqualError(t, s.ValidateRequired(Foo{ID: "x"}),
		"meta.Foo (foo) validation failed: missing required field Field1")
	require.EqualError(t, s.ValidateRequired(Foo{ID: "x", Field1: 1}),
		"meta.Foo (foo) validation failed: missing required field List1")
	require.EqualError(t, s.ValidateRequired(Foo{ID: "x", Field1: 1, List1: []int{}}),
		"meta.Foo (foo) validation failed: missing required field Map1")
	require.NoError(t, s.ValidateRequired(Foo{ID: "x", Field1: 1, List1: []int{}, Map1: map[string]int{}}))
}
