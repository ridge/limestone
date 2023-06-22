package kafkago

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/test"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockWriterAPI struct {
	nullAPI
	conn mockConn
}

func (m *mockWriterAPI) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (kafkaConn, error) {
	if topic != "test-topic" {
		return nil, kafka.InvalidTopic // non-retriable
	}
	return &m.conn, nil
}

func TestFatalError(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	msg := api.Message{
		Topic: "test-topic-nonexistent",
		Key:   "test-message",
		Value: []byte("test-message-value"),
	}

	require.Error(t, c.Write(ctx, "test-topic-nonexistent", []api.Message{msg}))

	mockAPI.conn.AssertExpectations(t)
}

func TestWriteSingleMessage(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	msg := api.Message{
		Topic: "test-topic",
		Key:   "test-message",
		Value: []byte("test-message-value"),
	}
	kafkaMsg := kafka.Message{
		Key:     []byte(msg.Key),
		Value:   msg.Value,
		Headers: []kafka.Header{},
	}

	mockAPI.conn.On("SetDeadline", matchAny()).Return(nil).Once()
	mockAPI.conn.On("WriteMessages", []kafka.Message{kafkaMsg}).Return(42, nil).Once()
	require.NoError(t, c.Write(ctx, "test-topic", []api.Message{msg}))

	mockAPI.conn.AssertExpectations(t)
}

func TestWriteMultipleMessages(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	for i := 0; i < 10; i++ {
		msg := api.Message{
			Topic: "test-topic",
			Key:   fmt.Sprintf("test-message-%d", i),
			Value: []byte(fmt.Sprintf("test-message-%d-value", i)),
		}
		kafkaMsg := kafka.Message{
			Key:     []byte(msg.Key),
			Topic:   "",
			Value:   msg.Value,
			Headers: []kafka.Header{},
		}

		mockAPI.conn.On("SetDeadline", matchAny()).Return(nil).Once()
		mockAPI.conn.On("WriteMessages", []kafka.Message{kafkaMsg}).Return(42, nil).Once()
		require.NoError(t, c.Write(ctx, "test-topic", []api.Message{msg}))
	}

	mockAPI.conn.AssertExpectations(t)
}

func TestWriteMultipleMessagesInCall(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	msgs := make([]api.Message, 0, 10)
	kafkaMsgs := make([]kafka.Message, 0, 10)
	for i := 0; i < 10; i++ {
		msg := api.Message{
			Topic: "test-topic",
			Key:   fmt.Sprintf("test-message-%d", i),
			Value: []byte(fmt.Sprintf("test-message-%d-value", i)),
		}
		kafkaMsg := kafka.Message{
			Key:     []byte(msg.Key),
			Topic:   "",
			Value:   msg.Value,
			Headers: []kafka.Header{},
		}

		msgs = append(msgs, msg)
		kafkaMsgs = append(kafkaMsgs, kafkaMsg)
	}

	mockAPI.conn.On("SetDeadline", matchAny()).Return(nil).Once()
	mockAPI.conn.On("WriteMessages", kafkaMsgs).Return(42, nil).Once()
	require.NoError(t, c.Write(ctx, "test-topic", msgs))

	mockAPI.conn.AssertExpectations(t)
}

func TestMessageWithHeaders(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	msg := api.Message{
		Topic: "test-topic",
		Key:   "test-message",
		Value: []byte("test-message-value"),
		Headers: map[string]string{
			"test-header": "test-header-value",
		},
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(msg.Key),
		Topic: "",
		Value: msg.Value,
		Headers: []kafka.Header{
			{
				Key:   "test-header",
				Value: []byte("test-header-value"),
			},
		},
	}

	mockAPI.conn.On("SetDeadline", matchAny()).Return(nil).Once()
	mockAPI.conn.On("WriteMessages", []kafka.Message{kafkaMsg}).Return(42, nil).Once()
	require.NoError(t, c.Write(ctx, "test-topic", []api.Message{msg}))

	mockAPI.conn.AssertExpectations(t)
}

func TestMessageSendNotOK(t *testing.T) {
	ctx := test.Context(t)

	var mockAPI mockWriterAPI
	c := newClient([]string{"localhost:6666", "localhost:7777"}, &mockAPI)

	msg := api.Message{
		Topic: "test-topic",
		Key:   "test-message",
		Value: []byte("test-message-value"),
	}
	kafkaMsg := kafka.Message{
		Key:     []byte(msg.Key),
		Topic:   "",
		Value:   msg.Value,
		Headers: []kafka.Header{},
	}

	ctx, cancel := context.WithCancel(ctx) // no retrying
	defer cancel()

	mockAPI.conn.On("SetDeadline", matchAny()).Return(nil).Once()
	mockAPI.conn.On("WriteMessages", []kafka.Message{kafkaMsg}).Return(0, errors.New("failed to send kafka message")).Once()
	mockAPI.conn.On("Close").Return(nil).Run(func(args mock.Arguments) { cancel() }).Once()
	require.Error(t, c.Write(ctx, "test-topic", []api.Message{msg}))

	mockAPI.conn.AssertExpectations(t)
}
