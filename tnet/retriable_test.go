package tnet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetriableNil(t *testing.T) {
	require.Nil(t, MaybeRetriableError(nil))
}
