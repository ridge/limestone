package thttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
)

const maxLogBodyLen = 1024 - 3 // make room for 3 dots

// LogBodies is a middleware that logs request and response bodies.
//
// Only has an effect when debug logging is enabled.
func LogBodies(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		logger := tlog.Get(req.Context())
		if !logger.Core().Enabled(zap.DebugLevel) {
			next.ServeHTTP(w, req)
			return
		}

		if shouldLogBody(req.Header) {
			req.Body = createReadCloserCapture(req.Body, func(p []byte, _ bool) {
				logger.Debug("HTTP request body", zap.String("contentType", contentType(req.Header)), zap.ByteString("requestData", p))
			})
		}

		crw := &captureResponseWriter{ResponseWriter: w}
		// FIXME (eyal): flusher interface is ignored
		if h, ok := w.(http.Hijacker); ok {
			crw.Hijacker = h
		}
		next.ServeHTTP(crw, req)
		if shouldLogBody(crw.ResponseWriter.Header()) {
			logger.Debug("HTTP response body", zap.String("contentType", contentType(crw.ResponseWriter.Header())), zap.ByteString("body", crw.buff.Bytes()))
		}
	})
}

func contentType(header http.Header) string {
	return strings.TrimSpace(strings.ToLower(header.Get("Content-Type")))
}

func shouldLogBody(header http.Header) bool {
	return contentType(header) != "application/octet-stream"
}

type captureReadCloser struct {
	rc   io.ReadCloser
	buff bytes.Buffer
	done func([]byte, bool)
}

func createReadCloserCapture(rc io.ReadCloser, done func([]byte, bool)) *captureReadCloser {
	if rc == nil {
		rc = http.NoBody
	}

	var captured bool
	doneOnce := func(p []byte, eof bool) {
		if captured {
			return
		}
		captured = true
		done(p, eof)
	}

	return &captureReadCloser{rc: rc, done: doneOnce}
}

func appendToBuffer(buff *bytes.Buffer, p []byte, n int) {
	remaining := maxLogBodyLen - buff.Len()
	if n == 0 || remaining <= 0 {
		return
	}
	if n > remaining {
		must.OK1(buff.Write(p[:remaining])) // must is safe because buffer.Write() always returns nil
		must.OK1(buff.WriteString("..."))
	} else {
		must.OK1(buff.Write(p[:n]))
	}
}

func (crc *captureReadCloser) Read(p []byte) (int, error) {
	n, err := crc.rc.Read(p)
	appendToBuffer(&crc.buff, p, n)
	if errors.Is(err, io.EOF) {
		crc.done(crc.buff.Bytes(), true)
	}
	return n, err
}

func (crc *captureReadCloser) Close() error {
	crc.done(crc.buff.Bytes(), false)
	return crc.rc.Close()
}

type captureResponseWriter struct {
	http.ResponseWriter
	http.Hijacker
	buff bytes.Buffer
}

func (crw *captureResponseWriter) Write(p []byte) (int, error) {
	n, err := crw.ResponseWriter.Write(p)
	appendToBuffer(&crw.buff, p, n)
	return n, err
}

// JSONResult writes HTTP error code and JSON
func JSONResult(logger *zap.Logger, writer http.ResponseWriter, res any, code int) {
	body := must.OK1(json.Marshal(res))
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(code)
	if _, err := writer.Write(body); err != nil {
		logger.Debug("failed to write response to client", zap.Error(err))
	}
}
