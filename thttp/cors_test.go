package thttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCORS(t *testing.T) {
	handler := CORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello"))
		assert.NoError(t, err)
	}))

	r := httptest.NewRequest(http.MethodOptions, "http://localhost", nil)
	r.Header.Set("Origin", "http://someorigin")
	r.Header.Set("Access-Control-Request-Method", http.MethodGet)
	res := Test(handler, r)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "*", strings.Join(res.Header["Access-Control-Allow-Origin"], ","))
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Empty(t, body)
	res.Body.Close()

	r = httptest.NewRequest(http.MethodGet, "http://localhost", nil)
	r.Header.Set("Origin", "http://someorigin")
	res = Test(handler, r)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, strings.Join(exposedHeaders, ","), strings.Join(res.Header["Access-Control-Expose-Headers"], ","))
	assert.Equal(t, "*", strings.Join(res.Header["Access-Control-Allow-Origin"], ","))
	body, err = io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
	res.Body.Close()
}
