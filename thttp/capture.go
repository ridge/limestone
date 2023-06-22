package thttp

import "net/http"

// CaptureStatus wraps a http.ResponseWriter to capture the response status code.
// The status code will be written into *status.
//
// The returned ResponseWriter works the same way as the original one, including
// the http.Hijacker functionality, if available.
func CaptureStatus(w http.ResponseWriter, status *int) http.ResponseWriter {
	cs := captureStatus{ResponseWriter: w, status: status}
	if h, ok := w.(http.Hijacker); ok {
		cs.Hijacker = h
	}
	return cs
}

type captureStatus struct {
	http.ResponseWriter
	http.Hijacker
	status *int
}

func (cs captureStatus) Write(b []byte) (int, error) {
	if *cs.status == 0 {
		*cs.status = http.StatusOK
	}
	return cs.ResponseWriter.Write(b)
}

func (cs captureStatus) WriteHeader(statusCode int) {
	*cs.status = statusCode
	cs.ResponseWriter.WriteHeader(statusCode)
}
