package thttp

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"os"

	"github.com/kevinpollet/nego"
	"github.com/ridge/must/v2"
)

func gzipCompress(r io.Reader) ([]byte, error) {
	compressed := bytes.NewBuffer(nil)
	compressor := gzip.NewWriter(compressed)
	if _, err := io.Copy(compressor, r); err != nil {
		return nil, err
	}
	if err := compressor.Close(); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

// ShouldGzip returns if gzip-compression is asked for in HTTP request
func ShouldGzip(r *http.Request) bool {
	// nego.NegotiateContentEncoding(r, "gzip") returns "gzip"
	// if there is no "Accept-Encoding" header there. Guard against it.
	return r.Header.Get("Accept-Encoding") != "" && nego.NegotiateContentEncoding(r, "gzip") == "gzip"
}

// GzipEnabledFileHandler returns HTTP handler that serves the file.
// If the HTTP request asks for gzip Content-Encoding, the handler serves the file gzipped.
func GzipEnabledFileHandler(filename string) func(w http.ResponseWriter, r *http.Request) {
	fh := must.OK1(os.Open(filename))
	modTime := must.OK1(fh.Stat()).ModTime()
	gzipCompressed := must.OK1(gzipCompress(fh))
	must.OK(fh.Close())

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Accept-Encoding")
		if ShouldGzip(r) {
			w.Header().Set("Content-Encoding", "gzip")

			// Override Content-Type detection in ServeContent. If it is not overwritten, then
			// ServeContent detects gzipped file and sets application/gzip.
			//
			// 'curl --compressed' deals with it just fine, but 'wget --compression=auto'
			// fails to decompress the file in this case.
			w.Header().Set("Content-Type", "application/octet-stream")

			http.ServeContent(w, r, "", modTime, bytes.NewReader(gzipCompressed))
		} else {
			http.ServeFile(w, r, filename)
		}
	}
}
