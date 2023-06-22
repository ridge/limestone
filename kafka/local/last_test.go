package local

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLastOffset(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	))

	offset, err := env.client.LastOffset(env.group.Context(), "foo")
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)

	offset, err = env.client.LastOffset(env.group.Context(), "bar")
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
}
