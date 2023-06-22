package typeddb

import (
	"testing"

	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

func TestEID(t *testing.T) {
	type Foo struct {
		meta.Meta `limestone:"name=foo"`
		ID        string `limestone:"identity"`
	}
	require.PanicsWithValue(t, "EID with an empty kind, id=xyzzy", func() { _ = EID{ID: "xyzzy"}.String() })
	require.Equal(t, "typeddb.Foo (foo): xyzzy", EID{Kind: KindOf(Foo{}), ID: "xyzzy"}.String())
}
