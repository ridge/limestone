package indices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexSingle(t *testing.T) {
	require.Equal(t, FieldIndex("Test"), Index("Test"))
	require.Equal(t, FieldIndex("Test", SkipZeros), Index("Test!"))
}

func TestIndexCompound(t *testing.T) {
	require.Equal(t, CompoundIndex(FieldIndex("A"), FieldIndex("B"), FieldIndex("C")),
		Index("A B C"))
}

func TestIndexConditional(t *testing.T) {
	// Conditional index contains a func, so it can't be compared directly
	require.Equal(t, "Field[!F]", Index("-F Field").Name())
	require.Equal(t, "Field[F]", Index("+F Field").Name())
}

func TestIndexUniq(t *testing.T) {
	require.Equal(t, UniquifyIndex(FieldIndex("Field")), UniqueIndex("Field"))
}
