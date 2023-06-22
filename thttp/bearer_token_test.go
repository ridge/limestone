package thttp

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBearerTokenOK(t *testing.T) {
	token, err := BearerToken(http.Header{"Authorization": []string{"Bearer TOKEN"}})
	require.NoError(t, err)
	require.Equal(t, "TOKEN", token)
}

func TestParseBearerTokenMissing(t *testing.T) {
	_, err := BearerToken(http.Header{})
	require.Equal(t, ErrMissingAuthToken, err)
}

func TestParseBearerTokenMalformed(t *testing.T) {
	_, err := BearerToken(http.Header{"Authorization": []string{"Bear TOKEN"}})
	require.IsType(t, ErrMalformedAuthHeader{}, err)
}
