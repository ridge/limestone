package tlog

import (
	"fmt"
	"testing"

	"github.com/ridge/must/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// New creates a top-level logger.
//
// The caller must call the returned cleanup function after using the logger.
func New(config Config) *zap.Logger {
	var encoderName string
	development := true
	switch config.Format {
	case FormatJSON:
		encoderName = "json"
		development = false
	case FormatText:
		var color bool
		switch config.Color {
		case ColorYes:
			color = true
		case ColorNo:
			color = false
		case ColorAuto:
			color = term.IsTerminal(unix.Stdout)
		default:
			panic(fmt.Errorf("unexpected --color value: %s", config.Color))
		}

		encoderName = fmt.Sprintf("%s;color=%t", consoleEncoderName, color)
	default:
		panic(fmt.Errorf("unexpected --log-format value: %s", config.Format))
	}

	level := zapcore.InfoLevel
	if config.Verbose {
		level = zapcore.DebugLevel
	}

	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      development,
		Encoding:         encoderName,
		EncoderConfig:    DefaultEncoderConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger := must.OK1(cfg.Build())

	if config.Name != "" {
		logger = logger.Named(config.Name)
	}

	return logger
}

// NewForTesting creates a logger for use in unit tests.
//
// The caller must call the returned cleanup function after using the logger.
func NewForTesting(t *testing.T) *zap.Logger {
	return New(Config{
		Name:    t.Name(),
		Format:  FormatText,
		Color:   ColorAuto,
		Verbose: true,
	})
}
