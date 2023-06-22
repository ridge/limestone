// Package union contains shared code for union types.
//
// In Go, one could use any to store values of various types. Union is
// different in a few important ways:
//
//   - Only a restricted set of types is admitted, whereas any can hold
//     any type.
//   - Union types serialize in a way that makes it possible to distinguish among
//     the possible types. Currently this is supported for types that serialize as
//     strings; a possible development is to support types that serialize as other
//     kinds of JSON nodes.
//   - Union types are indexable by Limestone (implement indices.IndexKey).
//
// Go isn't flexible enough to make creation of new union types fully automatic,
// so this package only contains a common foundation for building your own union
// type. See iam/entities/irealm for an example of how to define a union type
// based on this package.
package union

import (
	"bytes"
	"encoding"
	"fmt"
	"reflect"

	"github.com/ridge/limestone/indices"
	"github.com/ridge/must/v2"
)

type variant struct {
	tag string
	t   reflect.Type

	marshalText   func(any) ([]byte, error)
	unmarshalText func([]byte) (any, error)
	indexKey      func(any) ([]byte, bool)
}

// A Union is a definition of a union type
type Union struct {
	byTag  map[string]*variant
	byType map[reflect.Type]*variant
}

var interfaceTextMarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var interfaceTextUnmarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var interfaceIndexKey = reflect.TypeOf((*indices.IndexKey)(nil)).Elem()

// New creates a new Union based on the given variants. The keys of the given
// map should be tags, and the values should be examples of variant type values.
func New(variants map[string]any) Union {
	u := Union{
		byTag:  map[string]*variant{},
		byType: map[reflect.Type]*variant{},
	}
	for tag, example := range variants {
		t := reflect.TypeOf(example)
		v := &variant{
			tag: tag,
			t:   t,
		}
		u.byTag[tag] = v
		u.byType[t] = v

		switch {
		case t.Implements(interfaceTextMarshaler):
			v.marshalText = func(v any) ([]byte, error) {
				return v.(encoding.TextMarshaler).MarshalText()
			}
		case t.Kind() == reflect.String:
			v.marshalText = func(v any) ([]byte, error) {
				return []byte(reflect.ValueOf(v).String()), nil
			}
		case t.Size() == 0:
			v.marshalText = func(v any) ([]byte, error) {
				return nil, nil
			}
		}

		switch {
		case reflect.PointerTo(t).Implements(interfaceTextUnmarshaler):
			v.unmarshalText = func(text []byte) (any, error) {
				res := reflect.New(t)
				if err := res.Interface().(encoding.TextUnmarshaler).UnmarshalText(text); err != nil {
					return nil, err
				}
				return res.Elem().Interface(), nil
			}
		case t.Kind() == reflect.String:
			v.unmarshalText = func(text []byte) (any, error) {
				return reflect.ValueOf(string(text)).Convert(t).Interface(), nil
			}
		case t.Size() == 0:
			v.unmarshalText = func(text []byte) (any, error) {
				if text != nil {
					return nil, fmt.Errorf("failed to parse %s", v.tag)
				}
				return reflect.Zero(t).Interface(), nil
			}
		}

		switch {
		case t.Implements(interfaceIndexKey):
			v.indexKey = func(v any) ([]byte, bool) {
				return v.(indices.IndexKey).IndexKey()
			}
		case t.Kind() == reflect.String:
			v.indexKey = func(v any) ([]byte, bool) {
				s := reflect.ValueOf(v).String()
				return []byte(s + "\x00"), s != ""
			}
		case t.Size() == 0:
			v.indexKey = func(v any) ([]byte, bool) {
				return nil, false
			}
		}
	}
	return u
}

// Validate checks if the given value has one of the allowed types
func (u Union) Validate(v any) error {
	if v == nil {
		return nil
	}
	if u.byType[reflect.TypeOf(v)] == nil {
		return fmt.Errorf("type %T not in union", v)
	}
	return nil
}

// Type returns the type tag for the given value
func (u Union) Type(v any) string {
	if v == nil {
		return ""
	}
	return u.byType[reflect.TypeOf(v)].tag
}

// String is a helper for implementing fmt.Stringer for union types
func (u Union) String(v any) string {
	if v == nil {
		return ""
	}
	return string(must.OK1(u.MarshalText(v)))
}

// StringValue returns a string representation of the value without the type tag
// (useful for API representation)
func (u Union) StringValue(v any) string {
	if v == nil {
		return ""
	}
	return string(must.OK1(u.byType[reflect.TypeOf(v)].marshalText(v)))
}

// MarshalText is a helper for implementing encoding.TextMarshaler for union
// types
func (u Union) MarshalText(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	variant := u.byType[reflect.TypeOf(v)]
	var b bytes.Buffer
	res, err := variant.marshalText(v)
	if err != nil {
		return nil, err
	}
	b.WriteString(variant.tag)
	if res != nil {
		b.WriteByte(':')
		b.Write(res)
	}
	return b.Bytes(), nil
}

// UnmarshalText is a helper for implementing encoding.TextUnmarshaler for
// union types
func (u Union) UnmarshalText(text []byte) (any, error) {
	if len(text) == 0 {
		return nil, nil
	}
	tag, value, _ := bytes.Cut(text, []byte(":"))
	variant := u.byTag[string(tag)]
	if variant == nil {
		return nil, fmt.Errorf("tag %s not in union", tag)
	}
	return variant.unmarshalText(value)
}

// IndexKey is a helper for implementing IndexKey expected by Limestone indices
// for union types
func (u Union) IndexKey(v any) ([]byte, bool) {
	if v == nil {
		return []byte{0}, false
	}
	variant := u.byType[reflect.TypeOf(v)]
	var b bytes.Buffer
	res, _ := variant.indexKey(v)
	b.WriteString(variant.tag)
	b.WriteByte(0)
	b.Write(res)
	return b.Bytes(), true
}
