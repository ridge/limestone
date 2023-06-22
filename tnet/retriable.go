package tnet

import (
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"

	"github.com/ridge/limestone/retry"
)

// MaybeRetriableError converts given network error into
// retry.ErrRetriable if the network operation is retriable
func MaybeRetriableError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && errors.Is(urlErr, io.EOF) {
		return retry.Retriable(err)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.Temporary() {
		return retry.Retriable(err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return retry.Retriable(err)
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return retry.Retriable(err)
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return retry.Retriable(err)
	}
	if errors.Is(err, syscall.EHOSTUNREACH) {
		return retry.Retriable(err)
	}
	if errors.Is(err, syscall.EPIPE) {
		return retry.Retriable(err)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return retry.Retriable(err)
	}
	// This is unexported error coming from DNS code
	if strings.Contains(err.Error(), "server misbehaving") {
		return retry.Retriable(err)
	}
	// This is a error coming from somewhere deep inside net/http and
	// happening infrequently when a HTTP server sends a response before
	// HTTP client has finished sending the request.
	//
	// See https://github.com/golang/go/issues/31259
	//
	// The error is produced by fmt.Errorf, so it can't be type-checked and
	// we have to resort to string matching.
	if strings.Contains(err.Error(), "readLoopPeekFailLocked: ") {
		return retry.Retriable(err)
	}
	return err
}
