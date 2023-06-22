package names

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTopicName(t *testing.T) {
	require.Error(t, ValidateTopicName(""))
	require.Error(t, ValidateTopicName("."))
	require.Error(t, ValidateTopicName(".."))
	require.Error(t, ValidateTopicName(":"))
	require.Error(t, ValidateTopicName(strings.Repeat("x", 250)))

	require.NoError(t, ValidateTopicName("x"))
	require.NoError(t, ValidateTopicName("X"))
	require.NoError(t, ValidateTopicName("1"))
	require.NoError(t, ValidateTopicName(".-_"))
	require.NoError(t, ValidateTopicName(strings.Repeat("x", 249)))
}
