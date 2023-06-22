package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"time"
)

var testExpConfig = ExpConfig{
	Min:   1 * time.Minute,
	Max:   10 * time.Minute,
	Scale: 2.0,
}

func TestBackoff(t *testing.T) {
	backoff := NewExpBackoff(testExpConfig)
	assert.Equal(t, backoff.Backoff(), testExpConfig.Min)
	assert.Equal(t, backoff.Backoff(), 2*testExpConfig.Min)
	assert.Equal(t, backoff.Backoff(), 4*testExpConfig.Min)
	assert.Equal(t, backoff.Backoff(), 8*testExpConfig.Min)
	assert.Equal(t, backoff.Backoff(), testExpConfig.Max)
	assert.Equal(t, backoff.Backoff(), testExpConfig.Max)

	backoff.Reset()
	assert.Equal(t, backoff.Backoff(), testExpConfig.Min)
	assert.Equal(t, backoff.Backoff(), 2*testExpConfig.Min)
}
