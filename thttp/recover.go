package thttp

import (
	"context"
	"net/http"
	"runtime/debug"

	"github.com/ridge/parallel"
)

// runTask executes the task in the current goroutine, recovering from panics.
// A panic is returned as ErrPanic.
func runTask(ctx context.Context, task parallel.Task) (err error) {
	defer func() {
		if p := recover(); p != nil {
			panicErr := parallel.ErrPanic{Value: p, Stack: debug.Stack()}
			err = panicErr
		}
	}()
	return task(ctx)
}

// Recover is a middleware that catches and logs panics from HTTP handlers
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := runTask(r.Context(), func(ctx context.Context) error {
			next.ServeHTTP(w, r)
			return nil
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			select {
			case r.Context().Value(panicKey).(chan error) <- err:
			default:
			}
		}
	})
}
