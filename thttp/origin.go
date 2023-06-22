package thttp

import (
	"errors"
	"fmt"
	"net/http"
)

func getScheme(r *http.Request) (string, error) {
	p := r.Header.Get("X-Forwarded-Proto")
	switch p {
	case "":
		if r.TLS != nil {
			return "https", nil
		}
		return "http", nil
	case "http", "https":
		return p, nil
	default:
		return "", fmt.Errorf("unexpected X-Forwarded-Proto %q", p)
	}
}

// Origin returns the origin of HTTP request.
func Origin(r *http.Request) (string, error) {
	scheme, err := getScheme(r)
	if err != nil {
		return "", err
	}
	if r.Host == "" {
		return "", errors.New("missing Host header")
	}
	return scheme + "://" + r.Host, nil
}
