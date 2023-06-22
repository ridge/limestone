package kafkago

import (
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"time"
)

type mockConn struct {
	mock.Mock
}

func (m *mockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockConn) SetDeadline(deadline time.Time) error {
	args := m.Called(deadline)
	return args.Error(0)
}

func (m *mockConn) WriteMessages(batch ...kafka.Message) (int, error) {
	args := m.Called(batch)
	return args.Int(0), args.Error(1)
}

func (m *mockConn) ReadPartitions(topics ...string) ([]kafka.Partition, error) {
	args := m.Called(topics)
	return args.Get(0).([]kafka.Partition), args.Error(1)
}

func (m *mockConn) ReadLastOffset() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}
