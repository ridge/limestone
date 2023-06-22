package kafkago

import (
	"context"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockLastOffsetAPI struct {
	nullAPI
	conn mockConn
}

func (m *mockLastOffsetAPI) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (kafkaConn, error) {
	return &m.conn, nil
}

func TestLastOffset(t *testing.T) {
	ctx := test.Context(t)

	var api mockLastOffsetAPI
	api.conn.On("SetDeadline", mock.AnythingOfType("time.Time")).Return(nil)
	api.conn.On("ReadLastOffset").Return(int64(42), nil).Once()

	c := newClient([]string{"localhost:6666"}, &api)

	offset, err := c.LastOffset(ctx, "foo")
	require.NoError(t, err)
	require.Equal(t, int64(42), offset)

	api.conn.AssertExpectations(t)
}
