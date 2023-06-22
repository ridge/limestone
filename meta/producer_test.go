package meta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProducerRule(t *testing.T) {
	rule := ParseProducerSet("foo")
	require.Equal(t, ProducerSet{"foo": true}, rule)
	require.Equal(t, "foo", rule.String())

	rule = ParseProducerSet("foo|bar")
	require.Equal(t, ProducerSet{"foo": true, "bar": true}, rule)
	require.Equal(t, "bar|foo", rule.String())

	require.Panics(t, func() {
		ParseProducerSet("foo|foo")
	})
}
