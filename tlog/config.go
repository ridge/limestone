package tlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

// Format is the logging format
type Format string

// Format values
const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

// Color is the coloring setting for text format
type Color string

// Color values
const (
	ColorAuto Color = ""
	ColorYes  Color = "yes"
	ColorNo   Color = "no"
)

// Config is the configuration for creating a top-level logger
type Config struct {
	Name    string // top-level logger name (optional)
	Format  Format
	Color   Color
	Verbose bool // enable messages at Debug level
}

func iso8601MicroTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02T15:04:05.000000Z0700"))
}

// DefaultEncoderConfig is the default value of zap.EncoderConfig that we use
// when creating top-level loggers
var DefaultEncoderConfig = func() zapcore.EncoderConfig {
	ec := zap.NewProductionEncoderConfig()
	ec.EncodeTime = iso8601MicroTimeEncoder
	return ec
}()
