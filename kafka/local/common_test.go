package local

import (
	"os"
	"strings"
	"testing"

	"time"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/test"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

var testTime = time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)

type testEnv struct {
	group *parallel.Group
	dir   string

	client api.Client
}

func setupTest(t *testing.T) *testEnv {
	dir := must.OK1(os.MkdirTemp("", ""))
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	group := test.Group(t)

	client, err := New(dir)
	require.NoError(t, err)

	return &testEnv{
		group:  group,
		dir:    dir,
		client: client,
	}
}

// lines concatenates the strings, terminating each with a new line.
// The function prevents ugly multi-line string literals.
func lines(lines ...string) string {
	var b strings.Builder
	for _, l := range lines {
		must.OK1(b.WriteString(l))
		must.OK(b.WriteByte('\n'))
	}
	return b.String()
}

func appendFile(filename string, data string) {
	f := must.OK1(os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644))
	defer must.Do(f.Close)
	must.OK1(f.Write([]byte(data)))
}
