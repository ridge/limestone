package tcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"time"
)

func TestReopen(t *testing.T) {
	var key struct{}
	ctx1, cancel := context.WithTimeout(context.WithValue(context.Background(), &key, 42), time.Hour)

	ctx2 := Reopen(ctx1)
	assert.Equal(t, 42, ctx2.Value(&key))
	assert.Nil(t, ctx2.Err())
	_, hasDeadline := ctx2.Deadline()
	assert.False(t, hasDeadline)
	select {
	case <-ctx2.Done():
		assert.Fail(t, "context closed")
	default:
	}

	cancel()

	assert.Equal(t, 42, ctx2.Value(&key))
	assert.Nil(t, ctx2.Err())
	_, hasDeadline = ctx2.Deadline()
	assert.False(t, hasDeadline)
	select {
	case <-ctx2.Done():
		assert.Fail(t, "context closed")
	default:
	}

	ctx3 := Reopen(ctx1)
	assert.Equal(t, 42, ctx3.Value(&key))
	assert.Nil(t, ctx3.Err())
	_, hasDeadline = ctx3.Deadline()
	assert.False(t, hasDeadline)
	select {
	case <-ctx3.Done():
		assert.Fail(t, "context closed")
	default:
	}
}
