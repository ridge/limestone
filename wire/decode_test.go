package wire

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestDecodeField(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta    `limestone:"name=foo"`
		ID           fooID `limestone:"identity"`
		Field        int
		ConstField   int `limestone:"const"`
		RenamedField int `limestone:"name=SomethingElse"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Decode(s, nil, Diff{}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "Unknown": json.RawMessage(`"y"`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x"}, res)

	current := res

	res, err = Decode(s, current, Diff{"ID": json.RawMessage(`"x"`)}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Decode(s, current, Diff{}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	_, err = Decode(s, current, Diff{"ID": json.RawMessage(`"y"`)}, nil)
	require.Error(t, err)

	_, err = Decode(s, current, Diff{"ID": json.RawMessage(`"y"`), "ConstField": json.RawMessage(`42`)}, nil)
	require.Error(t, err)

	_, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "ConstField": json.RawMessage(`"42"`)}, nil)
	require.Error(t, err)

	res, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "ConstField": json.RawMessage(`42`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", ConstField: 42}, res)

	current = res

	res, err = Decode(s, current, Diff{"ConstField": json.RawMessage(`42`)}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	_, err = Decode(s, current, Diff{"ConstField": json.RawMessage(`7`)}, nil)
	require.Error(t, err)

	res, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "RenamedField": json.RawMessage(`7`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x"}, res)

	res, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "SomethingElse": json.RawMessage(`7`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", RenamedField: 7}, res)
}

func TestDecodeList(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		List      []int
		ConstList []int `limestone:"const"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "List": json.RawMessage(`[]`), "ConstList": json.RawMessage(`[]`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", List: []int{}, ConstList: []int{}}, res)

	current := res

	res, err = Decode(s, current, Diff{"List": json.RawMessage(`[]`), "ConstList": json.RawMessage(`[]`)}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Decode(s, current, Diff{"List": json.RawMessage(`[1,2]`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", List: []int{1, 2}, ConstList: []int{}}, res)

	current = res

	res, err = Decode(s, current, Diff{"List": json.RawMessage(`null`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", ConstList: []int{}}, res)

	current = res

	_, err = Decode(s, current, Diff{"ConstList": json.RawMessage(`[1,2]`)}, nil)
	require.Error(t, err)
}

func TestDecodeMap(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		Map       map[string]int
		ConstMap  map[string]int `limestone:"const"`
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	res, err := Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "Map": json.RawMessage(`{}`), "ConstMap": json.RawMessage(`{}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", Map: map[string]int{}, ConstMap: map[string]int{}}, res)

	current := res

	res, err = Decode(s, current, Diff{"Map": json.RawMessage(`{}`), "ConstMap": json.RawMessage(`{}`)}, nil)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = Decode(s, current, Diff{"Map": json.RawMessage(`{"a":1,"b":2}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", Map: map[string]int{"a": 1, "b": 2}, ConstMap: map[string]int{}}, res)

	current = res

	res, err = Decode(s, current, Diff{"Map": json.RawMessage(`null`)}, nil)
	require.NoError(t, err)
	require.Equal(t, foo{ID: "x", ConstMap: map[string]int{}}, res)

	current = res

	_, err = Decode(s, current, Diff{"ConstMap": json.RawMessage(`{"a":1,"b":2}`)}, nil)
	require.Error(t, err)
}

func TestDecodeValidate(t *testing.T) {
	type fooID string
	type foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        fooID `limestone:"identity"`
		Field     int
	}
	s := meta.Survey(reflect.TypeOf(foo{}))

	_, err := Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`)}, func(index int, v1, v2 reflect.Value) error {
		return nil
	})
	require.NoError(t, err)

	_, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`)}, func(index int, v1, v2 reflect.Value) error {
		return errors.New("foo")
	})
	require.EqualError(t, err, "update decoding failed: field ID of wire.foo (foo): foo")

	_, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "Field": json.RawMessage(`0`)}, func(index int, v1, v2 reflect.Value) error {
		if s.Fields[index].GoName == "Field" {
			return errors.New("foo")
		}
		return nil
	})
	require.NoError(t, err)

	_, err = Decode(s, nil, Diff{"ID": json.RawMessage(`"x"`), "Field": json.RawMessage(`42`)}, func(index int, v1, v2 reflect.Value) error {
		if s.Fields[index].GoName == "Field" {
			return errors.New("foo")
		}
		return nil
	})
	require.EqualError(t, err, "update decoding failed: field Field of wire.foo (foo): foo")
}
