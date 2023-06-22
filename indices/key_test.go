package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"time"
)

func TestKeyFnUnknown(t *testing.T) {
	require.Nil(t, keyFnForType(reflect.TypeOf((*int)(nil))))
}

func TestKeyFnBool(t *testing.T) {
	type Bool bool
	kfn := keyFnForType(reflect.TypeOf(Bool(false)))
	b, ok := kfn(reflect.ValueOf(Bool(false)))
	require.False(t, ok)
	require.Equal(t, []byte{0x00}, b)
	b, ok = kfn(reflect.ValueOf(Bool(true)))
	require.True(t, ok)
	require.Equal(t, []byte{0x01}, b)
}

func TestKeyFnUint(t *testing.T) {
	type UShort uint16
	kfn := keyFnForType(reflect.TypeOf(UShort(0)))
	b, ok := kfn(reflect.ValueOf(UShort(0)))
	require.False(t, ok)
	require.Equal(t, []byte{0x00, 0x00}, b)
	b, ok = kfn(reflect.ValueOf(UShort(258)))
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02}, b)
	b, ok = kfn(reflect.ValueOf(UShort(65535)))
	require.True(t, ok)
	require.Equal(t, []byte{0xff, 0xff}, b)
}

func TestKeyFnInt(t *testing.T) {
	type Short int16
	kfn := keyFnForType(reflect.TypeOf(Short(0)))
	b, ok := kfn(reflect.ValueOf(Short(0)))
	require.False(t, ok)
	require.Equal(t, []byte{0x80, 0x00}, b)
	b, ok = kfn(reflect.ValueOf(Short(258)))
	require.True(t, ok)
	require.Equal(t, []byte{0x81, 0x02}, b)
	b, ok = kfn(reflect.ValueOf(Short(-1)))
	require.True(t, ok)
	require.Equal(t, []byte{0x7f, 0xff}, b)
	b, ok = kfn(reflect.ValueOf(Short(-258)))
	require.True(t, ok)
	require.Equal(t, []byte{0x7e, 0xfe}, b)
	b, ok = kfn(reflect.ValueOf(Short(-32768)))
	require.True(t, ok)
	require.Equal(t, []byte{0x00, 0x00}, b)
}

func TestKeyFnString(t *testing.T) {
	type String string
	kfn := keyFnForType(reflect.TypeOf(String("")))
	b, ok := kfn(reflect.ValueOf(String("")))
	require.False(t, ok)
	require.Equal(t, []byte{0x00}, b)
	b, ok = kfn(reflect.ValueOf(String("!")))
	require.True(t, ok)
	require.Equal(t, []byte{0x21, 0x00}, b)
}

func TestKeyFnTime(t *testing.T) {
	kfn := keyFnForType(reflect.TypeOf(time.Time{}))
	b, ok := kfn(reflect.ValueOf(time.Time{}))
	require.False(t, ok)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, b)
	b, ok = kfn(reflect.ValueOf(time.Time{}.Add(1)))
	require.True(t, ok)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, b)
	b, ok = kfn(reflect.ValueOf(time.Time{}.Add(time.Second)))
	require.True(t, ok)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, b)
}

type testKey struct {
	b  []byte
	ok bool
}

func (tk testKey) IndexKey() ([]byte, bool) {
	return tk.b, tk.ok
}

func TestKeyFnCustom(t *testing.T) {
	kfn := keyFnForType(reflect.TypeOf(testKey{}))
	b, ok := kfn(reflect.ValueOf(testKey{}))
	require.False(t, ok)
	require.Equal(t, []byte(nil), b)
	b, ok = kfn(reflect.ValueOf(testKey{b: []byte{0x01, 0x02}, ok: true}))
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02}, b)
}
