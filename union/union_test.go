package union

import (
	"encoding/json"
	"testing"

	"github.com/ridge/must/v2"
	"github.com/stretchr/testify/require"
)

type testEmpty struct{}
type testID string

type testRef struct {
	value any
}

var testUnion = New(map[string]any{
	"empty": testEmpty{},
	"str":   testID(""),
})

func (r testRef) MarshalText() ([]byte, error) {
	return testUnion.MarshalText(r.value)
}

func (r *testRef) UnmarshalText(text []byte) error {
	var err error
	r.value, err = testUnion.UnmarshalText(text)
	return err
}

func TestSerialize(t *testing.T) {
	var data struct{ A, B, C testRef }
	data.A = testRef{value: testEmpty{}}
	data.B = testRef{value: testID("foo")}
	require.Equal(t, `{"A":"empty","B":"str:foo","C":""}`, string(must.OK1(json.Marshal(data))))
}

func TestParse(t *testing.T) {
	var data struct{ A, B, C testRef }
	require.NoError(t, json.Unmarshal([]byte(`{"A":"empty","B":"str:foo","C":""}`), &data))
	require.Equal(t, testRef{value: testEmpty{}}, data.A)
	require.Equal(t, testRef{value: testID("foo")}, data.B)
	require.Equal(t, testRef{}, data.C)
}

func TestIndex(t *testing.T) {
	b, ok := testUnion.IndexKey(testEmpty{})
	require.True(t, ok)
	require.Equal(t, []byte("empty\x00"), b)
	b, ok = testUnion.IndexKey(testID("foo"))
	require.True(t, ok)
	require.Equal(t, []byte("str\x00foo\x00"), b)
	b, ok = testUnion.IndexKey(nil)
	require.False(t, ok)
	require.Equal(t, []byte("\x00"), b)
}

func TestStringValue(t *testing.T) {
	require.Equal(t, "", testUnion.StringValue(nil))
	require.Equal(t, "", testUnion.StringValue(testEmpty{}))
	require.Equal(t, "foo", testUnion.StringValue(testID("foo")))
}
