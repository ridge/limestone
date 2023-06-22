package retry

import (
	"context"

	"time"
)

// Sleep waits for the sooner event between two:
// -- closing the context, the error associated with the context returned
// -- the duration to elapse, nil returned
// If duration is 0 or negative the function returns immediately
func Sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}
