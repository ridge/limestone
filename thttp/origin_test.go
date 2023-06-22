package thttp

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func testOrigin(t *testing.T, expected, host string, header http.Header, tls *tls.ConnectionState) {
	o, err := Origin(&http.Request{
		Host:       host,
		Header:     header,
		TLS:        tls,
		RequestURI: "/hello",
	})
	if expected == "" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, expected, o)
	}
}

func TestOrigin(t *testing.T) {
	testOrigin(t, "http://example.com", "example.com", nil, nil)
	testOrigin(t, "http://example.com", "example.com",
		http.Header{"X-Forwarded-Proto": {"http"}}, nil)
	testOrigin(t, "https://example.com", "example.com",
		http.Header{"X-Forwarded-Proto": {"https"}}, nil)
	testOrigin(t, "https://example.net", "example.net", nil, &tls.ConnectionState{})
	testOrigin(t, "", "example.com", http.Header{"X-Forwarded-Proto": {"gopher"}}, nil)
	testOrigin(t, "", "", nil, nil)
}
