package wire

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestEncodeField(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta    `limestone:"name=foo"`
		ID           fooID `limestone:"identity"`
		Field        string
		ConstField   string `limestone:"const"`
		RenamedField string `limestone:"name=SomethingElse"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Encode(s, nil, foo{}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	_, err = Encode(s, foo{}, foo{ID: "x"}, nil)
	require.Error(t, err)

	res, err = Encode(s, nil, foo{ID: "x", Field: "a", ConstField: "b", RenamedField: "c"}, nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"ID": json.RawMessage(`"x"`), "Field": json.RawMessage(`"a"`), "ConstField": json.RawMessage(`"b"`), "SomethingElse": json.RawMessage(`"c"`)}, res)

	res, err = Encode(s, foo{ID: "x", Field: "a", ConstField: "b"}, foo{ID: "x", Field: "a", ConstField: "b"}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Encode(s, foo{ID: "x", Field: "", ConstField: "b"}, foo{ID: "x", Field: "a", ConstField: "b"}, nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"Field": json.RawMessage(`"a"`)}, res)

	_, err = Encode(s, foo{ID: "x", Field: "a", ConstField: ""}, foo{ID: "x", Field: "a", ConstField: "b"}, nil)
	require.Error(t, err)
}

func TestEncodeList(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		List      []string
		ConstList []string `limestone:"const"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Encode(s,
		nil,
		foo{ID: "x", List: []string{"a", "b"}, ConstList: []string{"a", "b"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"ID": json.RawMessage(`"x"`), "List": json.RawMessage(`["a","b"]`), "ConstList": json.RawMessage(`["a","b"]`)}, res)

	res, err = Encode(s,
		foo{ID: "x", List: []string{"a", "b"}, ConstList: []string{"a", "b"}},
		foo{ID: "x", List: []string{"a", "b"}, ConstList: []string{"a", "b"}},
		nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Encode(s,
		foo{ID: "x"},
		foo{ID: "x", List: []string{}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"List": json.RawMessage(`[]`)}, res)

	res, err = Encode(s,
		foo{ID: "x", List: []string{}},
		foo{ID: "x"},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"List": json.RawMessage(`null`)}, res)

	res, err = Encode(s,
		foo{ID: "x", List: []string{"a"}},
		foo{ID: "x", List: []string{"b"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"List": json.RawMessage(`["b"]`)}, res)

	res, err = Encode(s,
		foo{ID: "x", List: []string{"a"}},
		foo{ID: "x", List: []string{"b"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"List": json.RawMessage(`["b"]`)}, res)

	_, err = Encode(s,
		foo{ID: "x"},
		foo{ID: "x", ConstList: []string{"b"}},
		nil)
	require.Error(t, err)

	_, err = Encode(s,
		foo{ID: "x", ConstList: []string{"b"}},
		foo{ID: "x"},
		nil)
	require.Error(t, err)

	_, err = Encode(s,
		foo{ID: "x", ConstList: []string{"a"}},
		foo{ID: "x", ConstList: []string{"b"}},
		nil)
	require.Error(t, err)
}

func TestEncodeMap(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		Map       map[string]string
		ConstMap  map[string]string `limestone:"const"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Encode(s,
		nil,
		foo{ID: "x", Map: map[string]string{"a": "b"}, ConstMap: map[string]string{"a": "b"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"ID": json.RawMessage(`"x"`), "Map": json.RawMessage(`{"a":"b"}`), "ConstMap": json.RawMessage(`{"a":"b"}`)}, res)

	res, err = Encode(s,
		foo{ID: "x", Map: map[string]string{"a": "b"}, ConstMap: map[string]string{"a": "b"}},
		foo{ID: "x", Map: map[string]string{"a": "b"}, ConstMap: map[string]string{"a": "b"}},
		nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Encode(s,
		foo{ID: "x"},
		foo{ID: "x", Map: map[string]string{}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"Map": json.RawMessage(`{}`)}, res)

	res, err = Encode(s,
		foo{ID: "x", Map: map[string]string{}},
		foo{ID: "x"},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"Map": json.RawMessage(`null`)}, res)

	res, err = Encode(s,
		nil,
		foo{ID: "x", Map: map[string]string{"a": "z"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"ID": json.RawMessage(`"x"`), "Map": json.RawMessage(`{"a":"z"}`)}, res)

	res, err = Encode(s,
		foo{ID: "x", Map: map[string]string{"a": "z"}},
		foo{ID: "x", Map: map[string]string{"a": "zz"}},
		nil)
	require.NoError(t, err)
	require.Equal(t, Diff{"Map": json.RawMessage(`{"a":"zz"}`)}, res)

	_, err = Encode(s,
		foo{ID: "x"},
		foo{ID: "x", ConstMap: map[string]string{"a": "z"}},
		nil)
	require.Error(t, err)

	_, err = Encode(s,
		foo{ID: "x", ConstMap: map[string]string{"a": "z"}},
		foo{ID: "x"},
		nil)
	require.Error(t, err)

	_, err = Encode(s,
		foo{ID: "x", ConstMap: map[string]string{"a": "z"}},
		foo{ID: "x", ConstMap: map[string]string{"a": "zz"}},
		nil)
	require.Error(t, err)
}

func TestEncodeValidate(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		Field     string
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	_, err := Encode(s,
		nil,
		foo{ID: "x", Field: "a"},
		func(index int, before, after reflect.Value) error {
			return nil
		})
	require.NoError(t, err)

	_, err = Encode(s,
		nil,
		foo{ID: "x", Field: "a"},
		func(index int, before, after reflect.Value) error {
			return errors.New("foo")
		})
	require.EqualError(t, err, "update encoding failed: field ID of wire.foo (foo): foo")

	_, err = Encode(s,
		nil,
		foo{ID: "x", Field: "a"},
		func(index int, before, after reflect.Value) error {
			if s.Fields[index].GoName == "Field" {
				return errors.New("foo")
			}
			return nil
		})
	require.EqualError(t, err, "update encoding failed: field Field of wire.foo (foo): foo")

	_, err = Encode(s,
		foo{ID: "x", Field: "a"},
		foo{ID: "x", Field: "b"},
		func(index int, before, after reflect.Value) error {
			if s.Fields[index].GoName == "Field" {
				return errors.New("foo")
			}
			return nil
		})
	require.EqualError(t, err, "update encoding failed: field Field of wire.foo (foo): foo")

	_, err = Encode(s,
		foo{ID: "x", Field: "a"},
		foo{ID: "x", Field: "a"},
		func(index int, before, after reflect.Value) error {
			if s.Fields[index].GoName == "Field" {
				return errors.New("foo")
			}
			return nil
		})
	require.NoError(t, err)
}
