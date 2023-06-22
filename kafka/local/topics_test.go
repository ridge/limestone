package local

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopics(t *testing.T) {
	env := setupTest(t)

	appendFile(env.dir+"/foo", lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
		`FOO-666`,
	))
	appendFile(env.dir+"/bar", lines(
		`{"TS":"2020-01-01T12:00:00Z","Headers":{"a":"b","c":"d"},"Len":3}`,
		`BAR`,
	))

	topics, err := env.client.Topics(env.group.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"bar", "foo"}, topics)
}
