package kafkago

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
	"time"
)

// This is the subset of the kafka-go API that we use, defined as mockable API

type kafkaAPI interface {
	NewReader(config kafka.ReaderConfig) kafkaReader
	DialContext(ctx context.Context, network string, address string) (kafkaConn, error)
	DialLeader(ctx context.Context, network string, address string, topic string, partition int) (kafkaConn, error)
}

type kafkaReader interface {
	Close() error
	SetOffset(offset int64) error
	ReadLag(ctx context.Context) (lag int64, err error)
	Lag() int64
	FetchMessage(ctx context.Context) (kafka.Message, error)
}

type kafkaConn interface {
	Close() error
	SetDeadline(t time.Time) error
	WriteMessages(batch ...kafka.Message) (int, error)
	ReadPartitions(topics ...string) ([]kafka.Partition, error)
	ReadLastOffset() (int64, error)
}

type realKafkaAPI struct{}

func (realKafkaAPI) NewReader(config kafka.ReaderConfig) kafkaReader {
	return kafka.NewReader(config)
}

func (realKafkaAPI) DialContext(ctx context.Context, network string, address string) (kafkaConn, error) {
	return kafka.DialContext(ctx, network, address)
}

func (realKafkaAPI) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (kafkaConn, error) {
	return kafka.DialLeader(ctx, network, address, topic, partition)
}

func shouldRetry(err error) bool {
	if errors.Is(err, kafka.Unknown) {
		return true
	}
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return true
	}
	return kerr.Timeout()
}
