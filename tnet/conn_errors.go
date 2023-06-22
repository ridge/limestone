package tnet

import (
	"errors"
	"io"
	"strings"
	"syscall"
)

// IsClosedConnectionError returns if the passed error is "closed network connection".
func IsClosedConnectionError(err error) bool {
	// error is not exported from net, so it can't be directly matched using errors.Is.
	return err != nil && strings.HasSuffix(err.Error(), "use of closed network connection")
}

// StripClosedConnectionError returns nil if the passed error is
// "closed network connection", and the original error otherwise.
//
// This is handy to decrease the amount of spam in logs, as "closed network
// connection" is a common error that happens every time a network connection
// is closed as a result of handling context cancellation.
func StripClosedConnectionError(err error) error {
	if IsClosedConnectionError(err) {
		return nil
	}
	return err
}

func ignorableErrorForCopying(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		IsClosedConnectionError(err) ||
		err.Error() == "operation was canceled" // legacy code in src/net/net.go
}

func stripIgnorableErrorForCopying(err error) error {
	if ignorableErrorForCopying(err) {
		return nil
	}
	return err
}
