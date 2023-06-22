package thttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// HandlerTransport is a http.Transport that shortcuts requests to a given http.Handler locally
type HandlerTransport struct {
	Context context.Context //nolint:containedctx // this struct has a context because net/http is pre-context
	Handler http.Handler
}

// RoundTrip is an implementation of http.RoundTripper
func (ht HandlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.WithContext(ht.Context)

	w := &bufferResponseWriter{
		buffer: bytes.NewBuffer(nil),
		header: http.Header{},
	}

	ht.Handler.ServeHTTP(w, req)

	if w.sentHeader == nil {
		w.WriteHeader(http.StatusOK)
	}

	return &http.Response{
		Request:    req,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		StatusCode: w.status,
		Status:     fmt.Sprintf("%d %s", w.status, http.StatusText(w.status)),
		Header:     w.sentHeader,
		Body:       io.NopCloser(w.buffer),
	}, nil
}

type bufferResponseWriter struct {
	header http.Header
	buffer *bytes.Buffer
	status int
	// changes to Header() after WriteHeader() are ignored, so we need to store a copy
	sentHeader http.Header
}

func (w *bufferResponseWriter) Header() http.Header {
	return w.header
}

func (w *bufferResponseWriter) Write(p []byte) (int, error) {
	if w.sentHeader == nil {
		w.WriteHeader(http.StatusOK)
	}
	return w.buffer.Write(p)
}

func (w *bufferResponseWriter) WriteHeader(status int) {
	if w.sentHeader != nil {
		panic("WriteHeader called twice")
	}
	w.status = status
	w.sentHeader = w.header.Clone()
}
