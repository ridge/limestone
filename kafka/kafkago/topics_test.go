package kafkago

import (
	"context"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type mockTopicsAPI struct {
	nullAPI
	conn mockConn
}

func (m *mockTopicsAPI) DialContext(ctx context.Context, network string, address string) (kafkaConn, error) {
	return &m.conn, nil
}

func TestTopics(t *testing.T) {
	ctx := test.Context(t)

	var api mockTopicsAPI
	api.conn.On("ReadPartitions", []string(nil)).Return([]kafka.Partition{
		{Topic: "foo", ID: 0},
		{Topic: "bar", ID: 0},
		{Topic: "foo", ID: 1},
	}, nil).Once()
	api.conn.On("Close").Return(nil)

	c := newClient([]string{"localhost:6666"}, &api)

	topics, err := c.Topics(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"bar", "foo"}, topics)

	api.conn.AssertExpectations(t)
}
