package meta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTag(t *testing.T) {
	require.Equal(t, "", option{}.String())
	require.Equal(t, "foo", option{key: "foo"}.String())
	require.Equal(t, "foo=", option{key: "foo="}.String())
	require.Equal(t, "foo=bar", option{key: "foo=", value: "bar"}.String())
}

func TestParseTag(t *testing.T) {
	require.Empty(t, parseTag(``))
	require.Empty(t, parseTag(`foo`))
	require.Empty(t, parseTag(`foo limestone:""`))
	require.Equal(t, []option{{key: "foo"}}, parseTag(`limestone:"foo"`))
	require.Equal(t, []option{{key: "foo"}, {key: "foo"}}, parseTag(`limestone:"foo,foo"`))
	require.Equal(t, []option{{key: "foo"}, {key: "bar"}}, parseTag(`limestone:"foo,bar"`))
	require.Equal(t, []option{{key: "foo"}, {key: "bar=", value: "qux"}}, parseTag(`limestone:"foo,bar=qux"`))
	require.Equal(t, []option{{key: "foo"}, {key: "bar="}}, parseTag(`limestone:"foo,bar="`))
	require.Equal(t, []option{{key: "foo=", value: "bar=baz"}}, parseTag(`limestone:"foo=bar=baz"`))
	require.Equal(t, []option{{key: "foo=", value: "1"}, {key: "bar=", value: "2"}}, parseTag(`limestone:"foo=1,bar=2"`))
}
