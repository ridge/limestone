package kafkago

import (
	"context"
	"fmt"
	"testing"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/test"
	"github.com/ridge/parallel"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"time"
)

type mockReader struct {
	mock.Mock
}

func (m *mockReader) ReadLag(ctx context.Context) (lag int64, err error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockReader) Lag() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *mockReader) Close() error {
	return nil
}

func (m *mockReader) SetOffset(offset int64) error {
	args := m.Called(offset)
	return args.Error(0)
}

type mockReaderAPI struct {
	nullAPI
	reader mockReader
}

func (m *mockReaderAPI) NewReader(config kafka.ReaderConfig) kafkaReader {
	return &m.reader
}

func TestReadOneMessage(t *testing.T) {
	group := test.Group(t)

	var mockAPI mockReaderAPI

	ts := time.Now()
	recvMsg := kafka.Message{
		Topic:  "test-topic",
		Offset: 0,
		Key:    []byte("test-message"),
		Headers: []kafka.Header{{
			Key:   "test-header-key",
			Value: []byte("test-header-value"),
		}},
		Value: []byte("test-message-value"),
		Time:  ts,
	}

	mockAPI.reader.On("SetOffset", matchAny()).Return(nil).Once()
	mockAPI.reader.On("ReadLag", matchAny()).Return(int64(1), nil).Once()
	mockAPI.reader.On("FetchMessage", matchAny()).Return(recvMsg, nil).Once()
	mockAPI.reader.On("Lag").Return(int64(0), nil).Once()
	mockAPI.reader.On("FetchMessage", matchAny()).Return(kafka.Message{}, context.Canceled).Maybe()

	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)
	incoming := make(chan *api.IncomingMessage)

	group.Spawn("reader", parallel.Continue, func(ctx context.Context) error {
		return c.Read(group.Context(), "test-topic", 0, incoming)
	})

	// first read returns the data
	require.Equal(t, &api.IncomingMessage{
		Message: api.Message{
			Topic:   "test-topic",
			Key:     "test-message",
			Headers: map[string]string{"test-header-key": "test-header-value"},
			Value:   []byte("test-message-value"),
		},
		Time:   ts,
		Offset: 0,
	}, <-incoming)

	// second read returns nil as the lag reaches zero
	require.Nil(t, <-incoming)

	mockAPI.reader.AssertExpectations(t)
}

func TestReadManySingleMessages(t *testing.T) {
	group := test.Group(t)

	var mockAPI mockReaderAPI

	ts := time.Now()

	mockAPI.reader.On("SetOffset", matchAny()).Return(nil).Once()
	mockAPI.reader.On("ReadLag", matchAny()).Return(int64(1), nil).Once()

	for i := 0; i < 10; i++ {
		recvMsg := kafka.Message{
			Topic:  "test-topic",
			Offset: int64(i),
			Key:    []byte(fmt.Sprintf("test-message-%d", i)),
			Headers: []kafka.Header{{
				Key:   "test-header-key",
				Value: []byte("test-header-value"),
			}},
			Value: []byte(fmt.Sprintf("test-message-%d-value", i)),
			Time:  ts,
		}
		mockAPI.reader.On("FetchMessage", matchAny()).Return(recvMsg, nil).Once()
		mockAPI.reader.On("Lag").Return(int64(0), nil).Once()
	}
	mockAPI.reader.On("FetchMessage", matchAny()).Return(kafka.Message{}, context.Canceled).Maybe()

	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)
	incoming := make(chan *api.IncomingMessage)

	group.Spawn("reader", parallel.Continue, func(ctx context.Context) error {
		return c.Read(group.Context(), "test-topic", 0, incoming)
	})

	for i := 0; i < 10; i++ {
		// first read returns the data
		require.Equal(t, &api.IncomingMessage{
			Message: api.Message{
				Topic:   "test-topic",
				Key:     fmt.Sprintf("test-message-%d", i),
				Headers: map[string]string{"test-header-key": "test-header-value"},
				Value:   []byte(fmt.Sprintf("test-message-%d-value", i)),
			},
			Time:   ts,
			Offset: int64(i),
		}, <-incoming)

		// second read returns nil as the lag reaches zero
		require.Nil(t, <-incoming)
	}

	mockAPI.reader.AssertExpectations(t)
}

func TestReadManyMessagesInSingleBatch(t *testing.T) {
	group := test.Group(t)

	var mockAPI mockReaderAPI

	ts := time.Now()

	mockAPI.reader.On("SetOffset", matchAny()).Return(nil).Once()
	mockAPI.reader.On("ReadLag", matchAny()).Return(int64(10), nil).Once()

	for i := 0; i < 10; i++ {
		recvMsg := kafka.Message{
			Topic:  "test-topic",
			Offset: int64(i),
			Key:    []byte(fmt.Sprintf("test-message-%d", i)),
			Headers: []kafka.Header{{
				Key:   "test-header-key",
				Value: []byte("test-header-value"),
			}},
			Value: []byte(fmt.Sprintf("test-message-%d-value", i)),
			Time:  ts,
		}
		mockAPI.reader.On("FetchMessage", matchAny()).Return(recvMsg, nil).Once()
		mockAPI.reader.On("Lag").Return(int64(9-i), nil).Once()
	}
	mockAPI.reader.On("FetchMessage", matchAny()).Return(kafka.Message{}, context.Canceled).Maybe()

	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)
	incoming := make(chan *api.IncomingMessage)

	group.Spawn("reader", parallel.Continue, func(ctx context.Context) error {
		return c.Read(group.Context(), "test-topic", 0, incoming)
	})

	for i := 0; i < 10; i++ {
		require.Equal(t, &api.IncomingMessage{
			Message: api.Message{
				Topic:   "test-topic",
				Key:     fmt.Sprintf("test-message-%d", i),
				Headers: map[string]string{"test-header-key": "test-header-value"},
				Value:   []byte(fmt.Sprintf("test-message-%d-value", i)),
			},
			Time:   ts,
			Offset: int64(i),
		}, <-incoming)
	}
	// last read returns nil as the lag reaches zero
	require.Nil(t, <-incoming)
}
