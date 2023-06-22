package thttp

import (
	"io"
	"net/http"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	group := test.Group(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello"))
		assert.NoError(t, err)
	})

	s := NewServer(tnet.ListenOnRandomPort(), StandardMiddleware(handler))
	group.Spawn("server", parallel.Fail, s.Run)

	res, err := http.DefaultClient.Do(must.OK1(http.NewRequestWithContext(group.Context(), http.MethodGet, "http://"+s.ListenAddr().String(), nil)))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
	res.Body.Close()
}
