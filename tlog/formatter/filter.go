package formatter

// Keep this package independent from lib/run, otherwise the following happens:
//
// - log-filter uses lib/tlog/filter
// - lib/tlog/filter uses lib/run
// - lib/run reads TECTONIC_RUN_TARGET
// - log-filter stops working if TECTONIC_RUN_TARGET is set to a value it does not understand
// - Box behaves weirdly

import (
	"bufio"
	"errors"
	"io"

	"go.uber.org/zap/buffer"
)

// Stream takes logs from reader, transforms them to console format and writes to writer
func Stream(reader io.Reader, writer io.Writer, color bool) error {
	r := bufio.NewReader(reader)

	var prevTimestamp string
	for {
		b, err := r.ReadBytes('\n')

		// If r has read an incomplete line (e.g. due to missing EOL at
		// the end of the file) it will return both a buffer and an
		// error, so we can't test the error to see if we need to print
		// the line.
		if b != nil {
			var buffer *buffer.Buffer
			var err error
			buffer, prevTimestamp, err = JSONLogMessage(b, prevTimestamp, color)
			if err != nil {
				// Fall back to printing raw message
				if _, err := writer.Write(b); err != nil {
					return err
				}
			} else {
				if _, err := writer.Write(buffer.Bytes()); err != nil {
					buffer.Free()
					return err
				}
				buffer.Free()
			}
		}

		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
