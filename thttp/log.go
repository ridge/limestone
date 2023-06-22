package thttp

import (
	"net/http"

	"time"

	"github.com/ridge/limestone/tlog"
	"go.uber.org/zap"
)

// Log is a middleware that logs before and after handling of each request.
// Does not include logging of request and response bodies.
func Log(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		ctx := tlog.With(r.Context(),
			zap.String("method", r.Method),
			zap.String("hostname", r.Host),
			zap.String("url", r.URL.String()),
		)
		logger := tlog.Get(ctx)
		logger.Debug("HTTP request handling started")
		var status int
		next.ServeHTTP(CaptureStatus(w, &status), r.WithContext(ctx))
		logger.Debug("HTTP request handling ended", zap.Int("statusCode", status), zap.Duration("elapsed", time.Since(started)))
	})
}
