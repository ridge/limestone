package thttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

func oopsHandler(w http.ResponseWriter, r *http.Request) {
	panic(errors.New("oops"))
}

func TestRecover(t *testing.T) {
	group := test.Group(t)

	handler := Recover(http.HandlerFunc(oopsHandler))
	server := NewServer(tnet.ListenOnRandomPort(), handler)
	serverErr := make(chan error, 1)

	group.Spawn("server", parallel.Continue, func(ctx context.Context) error {
		serverErr <- server.Run(ctx)
		return nil
	})

	reqBody := io.NopCloser(strings.NewReader("hello"))
	req := must.OK1(http.NewRequestWithContext(group.Context(), http.MethodPost, fmt.Sprintf("http://%s", server.ListenAddr()), reqBody))
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, res.StatusCode)
	resBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, resBody)
	res.Body.Close()

	err = <-serverErr
	require.Error(t, err)
	require.EqualError(t, err, "panic: oops")
	var errPanic parallel.ErrPanic
	require.ErrorAs(t, err, &errPanic)
	require.Equal(t, errors.New("oops"), errPanic.Unwrap())
	require.Equal(t, errors.New("oops"), errPanic.Value)
	// oopsHandler must be mentioned: the stack is that of the panic location,
	// not where the panic is collected
	require.Regexp(t, "(?s)^goroutine.*oopsHandler", string(errPanic.Stack))
}
