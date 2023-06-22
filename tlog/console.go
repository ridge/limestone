package tlog

import (
	"fmt"
	"sync"

	"github.com/ridge/limestone/tlog/formatter"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// This encoder intentionally builds on top of the JSON encoder to ensure
// that the logs formatted by both this encoder and log-filter are identical
// in appearance.
//
// The performance is not stellar, but it should not be a problem in practice.

func init() {
	for _, color := range []bool{false, true} {
		color := color
		name := fmt.Sprintf("%s;color=%t", consoleEncoderName, color)
		must.OK(zap.RegisterEncoder(name, func(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
			return newConsoleEncoder(cfg, color), nil
		}))
	}
}

const consoleEncoderName = "tectonic-console"

type consoleEncoder struct {
	zapcore.Encoder // embedded JSON encoder to delegate formatting to
	color           bool

	lastTimestampMux sync.Mutex // Replace with atomic.Value if performance is a problem
	lastTimestamp    string
}

// Clone implements zapcore.Encoder
func (ce *consoleEncoder) Clone() zapcore.Encoder {
	return &consoleEncoder{
		Encoder: ce.Encoder.Clone(),
		color:   ce.color,
	}
}

// EncodeEntry implements zapcore.Encoder
func (ce *consoleEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	jsonBuf, err := ce.Encoder.EncodeEntry(entry, fields)
	if err != nil {
		return nil, err
	}
	defer jsonBuf.Free()

	ce.lastTimestampMux.Lock()
	defer ce.lastTimestampMux.Unlock()

	buffer, timestamp, err := formatter.JSONLogMessage(jsonBuf.Bytes(), ce.lastTimestamp, ce.color)
	if err != nil {
		return nil, err
	}
	ce.lastTimestamp = timestamp
	return buffer, err
}

func newConsoleEncoder(cfg zapcore.EncoderConfig, color bool) *consoleEncoder {
	return &consoleEncoder{
		Encoder: zapcore.NewJSONEncoder(cfg),
		color:   color,
	}
}
