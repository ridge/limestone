package test

import (
	"context"
	"errors"
	"testing"

	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
	"time"
)

// Group returns a parallel.Group with a testing context.
//
// If your code relies on the values normally injected into the context by Tool
// or Server, it's a good idea to test it with TestGroup to provide adequate
// replacements.
//
// If the group finishes with an error other than context.Canceled, the test is
// failed.
func Group(t *testing.T) *parallel.Group {
	group := parallel.NewGroup(Context(t))
	t.Cleanup(func() {
		group.Exit(nil)
		if err := group.Wait(); !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	})
	return group
}

// GroupWithTimeout is a version of TestGroup with a timeout.
//
// If the timeout expires, the test context is closed with
// context.DeadlineExceeded.
func GroupWithTimeout(t *testing.T, timeout time.Duration) *parallel.Group {
	group := parallel.NewGroup(ContextWithTimeout(t, timeout))
	t.Cleanup(func() {
		group.Exit(nil)
		if err := group.Wait(); !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	})
	return group
}
