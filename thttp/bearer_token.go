package thttp

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const bearerPrefix = "Bearer "

// ErrMissingAuthToken is an error return by BearerToken if there is no Authorization HTTP header
var ErrMissingAuthToken = errors.New("missing authentication token")

// ErrMalformedAuthHeader is an error returned by BearerToken if Authorization HTTP header is not in form "Bearer token"
type ErrMalformedAuthHeader struct {
	header string
}

func (e ErrMalformedAuthHeader) Error() string {
	return fmt.Sprintf("malformed authentication header: %q", e.header)
}

// BearerToken returns a bearer token, or an error if it is not found
func BearerToken(header http.Header) (string, error) {
	h := header.Get("Authorization")
	if h == "" {
		return "", ErrMissingAuthToken
	}
	bearer, ok := strings.CutPrefix(h, bearerPrefix)
	if !ok {
		return "", ErrMalformedAuthHeader{h}
	}
	return bearer, nil
}
