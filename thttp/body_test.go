package thttp

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

var testBytes = []byte("this is a body")

func TestCaptureReadCloserReadAll(t *testing.T) {
	var captured bool

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.True(t, eof)
		require.Equal(t, testBytes, readBytes)
	})

	readBytes, err := io.ReadAll(crc)
	require.NoError(t, err)
	require.Equal(t, testBytes, readBytes)
	require.True(t, captured)

	require.NoError(t, crc.Close())
}

func TestCaptureReadCloserReadPart(t *testing.T) {
	var captured bool
	bytesToRead := 3

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.False(t, eof)
		require.Equal(t, testBytes[:bytesToRead*2], readBytes)
	})

	readBytes := make([]byte, bytesToRead)
	n, err := crc.Read(readBytes)
	require.NoError(t, err)
	require.Equal(t, bytesToRead, n)
	require.Equal(t, testBytes[0:bytesToRead], readBytes)
	require.False(t, captured)

	n, err = crc.Read(readBytes)
	require.NoError(t, err)
	require.Equal(t, bytesToRead, n)
	require.Equal(t, testBytes[bytesToRead:bytesToRead*2], readBytes)
	require.False(t, captured)

	require.NoError(t, crc.Close())
	require.True(t, captured)
}

func TestCaptureReadCloserCloseWithoutReading(t *testing.T) {
	var captured bool

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.Nil(t, readBytes)
		require.False(t, eof)
	})

	require.NoError(t, crc.Close())
	require.True(t, captured)
}

func newCaptureReadCloser() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(testBytes))
}
