package tlog

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey int

const (
	tlogKey contextKey = iota
)

// Get returns a logger from context
func Get(ctx context.Context) *zap.Logger {
	return ctx.Value(tlogKey).(*zap.Logger)
}

// WithLogger adds a logger to a context or replaces an existing one
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, tlogKey, logger)
}

// With returns a context with a sub-logger with passed parameters
func With(ctx context.Context, fields ...zapcore.Field) context.Context {
	return WithLogger(ctx, Get(ctx).With(fields...))
}
