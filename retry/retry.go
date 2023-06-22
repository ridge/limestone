package retry

import (
	"context"
	"errors"

	"github.com/ridge/limestone/tlog"
	"go.uber.org/zap"
	"time"
)

// DelayFn is the type of function that can be called repeatedly to produce
// delays between attempts. A single value of DelayFn represents a single
// sequence of delays.
//
// Each call returns the delay before the next attempt, and a boolean value to
// indicate whether the next attempt is desired. If ok is false, the caller
// should stop trying and ignore the returned delay value. The caller is not
// expected to call the function again after receiving false.
//
// In other words, a DelayFn returns a finite or infinite sequence of delays
// over multiple calls. A false value of the second return value means the end
// of the sequence.
//
// -- Hey delay function, should I make an attempt?
// -- Yes, in two seconds (2*time.Second, true).
// -- Hey delay function, should I make an attempt?
// -- Yes, in four seconds (4*time.Second, true).
// -- Hey delay function, should I make an attempt?
// -- No (0, false).
//
// The delay function must return true as ok from the first call.
//
// Note that the first delay returned by the function is used before the very
// first attempt. For this reason, in most cases, the first call should return
// (0, true).
type DelayFn func() (delay time.Duration, ok bool)

// Config defines retry intervals.
//
// An implementation of Config is normally stateless.
type Config interface {
	// Delays returns a DelayFn representing the sequence of delays to use between attempts.
	// Each call to Delays returns a DelayFn representing an independent sequence.
	Delays() DelayFn
}

// FixedConfig defines fixed retry intervals
type FixedConfig struct {
	// TryAfter is the delay before the first attempt
	TryAfter time.Duration

	// RetryAfter is the delay before each subsequent attempt
	RetryAfter time.Duration

	// MaxAttempts is the maximum number of attempts taken; 0 = unlimited
	MaxAttempts int
}

// Delays implements interface Config
func (c FixedConfig) Delays() DelayFn {
	attempts := 0
	return func() (time.Duration, bool) {
		attempts++
		switch {
		case attempts == 1:
			return c.TryAfter, true
		case c.MaxAttempts != 0 && attempts > c.MaxAttempts:
			return 0, false
		default:
			return c.RetryAfter, true
		}
	}
}

// ErrRetriable means the operation that caused the error should be retried.
type ErrRetriable struct {
	err error
}

func (r ErrRetriable) Error() string {
	return r.err.Error()
}

// Unwrap returns the next error in the error chain.
func (r ErrRetriable) Unwrap() error {
	return r.err
}

// Retriable wraps an error to tell Do and DoWithTimeout that it should keep trying.
// Returns nil if err is nil.
func Retriable(err error) error {
	if err == nil {
		return nil
	}
	return ErrRetriable{err: err}
}

// Do executes the given function, retrying if necessary.
//
// The given Config is used to calculate the delays before each attempt.
//
// Wrap an error with Retriable to indicate that Do should try again.
//
// If the function returns success, or an error that isn't wrapped,
// Do returns that value immediately without trying more.
//
// A ErrRetriable error will be logged unless its message is exactly the same as
// the previous one.
func Do(ctx context.Context, c Config, f func() error) error {
	startedAt := time.Now()
	delays := c.Delays()
	var lastMessage string
	var r ErrRetriable
	for i := 0; ; i++ {
		logger := tlog.Get(ctx).With(zap.Int("attempts", i+1))

		delay, ok := delays()
		if !ok {
			if i == 0 {
				panic("ok is false on first attempt")
			}
			logger.Debug("Retry failed after maximum number of attempts", zap.Error(r.err), zap.Duration("duration", time.Since(startedAt)))
			return r.err
		}

		if err := Sleep(ctx, delay); err != nil {
			if i > 0 {
				logger.Debug("Retry canceled", zap.Error(err), zap.Duration("duration", time.Since(startedAt)))
			}
			return err
		}

		if err := f(); !errors.As(err, &r) {
			if i > 0 {
				if err != nil {
					logger.Debug("Retry finished with non-retriable error", zap.Error(err), zap.Duration("duration", time.Since(startedAt)))
				} else {
					logger.Debug("Retry succeeded", zap.Duration("duration", time.Since(startedAt)))
				}
			}
			return err
		}
		if errors.Is(r.err, ctx.Err()) {
			if i > 0 {
				logger.Debug("Retry canceled", zap.Error(r.err), zap.Duration("duration", time.Since(startedAt)))
			}
			return r.err // f wants to retry but the context is closing
		}

		newMessage := r.err.Error()
		if lastMessage != newMessage {
			logger.Debug("Will retry", zap.Error(r.err))
			lastMessage = newMessage
		}
	}
}

// DoWithTimeout is the same as Do, but uses an explicit timeout
func DoWithTimeout(ctx context.Context, c Config, timeout time.Duration, f func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return Do(ctx, c, func() error {
		return f(ctx)
	})
}

// Do1 is a single return value version of Do
func Do1[T any](ctx context.Context, c Config, f func() (T, error)) (T, error) {
	var t T
	err := Do(ctx, c, func() error {
		var err error
		t, err = f()
		return err
	})
	return t, err
}

// Do1WithTimeout is a single return value version of DoWithTimeout
func Do1WithTimeout[T any](ctx context.Context, c Config, timeout time.Duration, f func(ctx context.Context) (T, error)) (T, error) {
	var t T
	err := DoWithTimeout(ctx, c, timeout, func(ctx context.Context) error {
		var err error
		t, err = f(ctx)
		return err
	})
	return t, err
}

// Do2 is a two-return-values version of Do
func Do2[T1, T2 any](ctx context.Context, c Config, f func() (T1, T2, error)) (T1, T2, error) {
	var t1 T1
	var t2 T2
	err := Do(ctx, c, func() error {
		var err error
		t1, t2, err = f()
		return err
	})
	return t1, t2, err
}
