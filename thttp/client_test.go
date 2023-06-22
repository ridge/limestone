package thttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/stretchr/testify/assert"
)

func TestTest(t *testing.T) {
	ctx := test.Context(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello"))
		assert.NoError(t, err)
	})

	res := TestCtx(ctx, StandardMiddleware(handler), httptest.NewRequest(http.MethodGet, "/", nil))
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
}
