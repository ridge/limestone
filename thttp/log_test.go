package thttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	ctx := test.Context(t)

	handler := Log(LogBodies(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		_, err = w.Write(data)
		assert.NoError(t, err)
	})))

	r := httptest.NewRequest(http.MethodPost, "http://localhost", nil).WithContext(ctx)
	r.Body = io.NopCloser(strings.NewReader("hello"))
	res := TestCtx(ctx, handler, r)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
	res.Body.Close()
}
