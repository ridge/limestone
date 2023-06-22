package indices

import (
	"encoding/binary"
	"reflect"

	"time"
)

// A keyFn serializes a type to a sortable byte sequence
type keyFn func(reflect.Value) ([]byte, bool)

// IndexKey is an interface that can be implemented to make any type indexable
type IndexKey interface {
	IndexKey() ([]byte, bool)
}

var indexKeyInterface = reflect.TypeOf((*IndexKey)(nil)).Elem()

var timeType = reflect.TypeOf(time.Time{})

var unixSecondsOffset = time.Time{}.Unix()

func keyFnForType(t reflect.Type) keyFn {
	if t.Implements(indexKeyInterface) {
		return func(v reflect.Value) ([]byte, bool) {
			return v.Interface().(IndexKey).IndexKey()
		}
	}
	if t == timeType {
		return func(v reflect.Value) ([]byte, bool) {
			// We want to represent the time as a monotonic unsigned number so
			// that the index value of time.Time{} is all zeros.
			//
			// UnixNano() does not return 0 for time.Time{}.
			//
			// t.Sub(time.Time{}) doesn't fit into int64.
			//
			// So we represent the time close to its internal format: as a pair
			// of seconds (64 bits) and nanoseconds (0-999999999, 32 bits).
			// Unix() returns the number of seconds but needs offset correction
			// so that it's 0 for time.Time{}.
			b := make([]byte, 12)
			t := v.Interface().(time.Time)
			binary.BigEndian.PutUint64(b[:8], uint64(t.Unix()-unixSecondsOffset))
			binary.BigEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
			return b, !t.IsZero()
		}
	}
	switch t.Kind() {
	case reflect.Bool:
		return func(v reflect.Value) ([]byte, bool) {
			if v.Bool() {
				return []byte{1}, true
			}
			return []byte{0}, false
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		offset := 8 - t.Size()
		return func(v reflect.Value) ([]byte, bool) {
			var b [8]byte
			n := v.Uint()
			binary.BigEndian.PutUint64(b[:], n)
			return b[offset:], n != 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		offset := 8 - t.Size()
		return func(v reflect.Value) ([]byte, bool) {
			var b [8]byte
			n := v.Int()
			binary.BigEndian.PutUint64(b[:], uint64(n))
			// inverting the sign bit makes the serializations sort naturally
			b[offset] ^= 0x80
			return b[offset:], n != 0
		}
	case reflect.String:
		return func(v reflect.Value) ([]byte, bool) {
			s := v.String()
			return []byte(s + "\x00"), s != ""
		}
	default:
		return nil
	}
}
