package kafkago

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

type nullAPI struct{}

func (nullAPI) NewReader(config kafka.ReaderConfig) kafkaReader {
	panic("not implemented")
}

func (nullAPI) DialContext(ctx context.Context, network string, address string) (kafkaConn, error) {
	panic("not implemented")
}

func (nullAPI) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (kafkaConn, error) {
	panic("not implemented")
}

func matchAny() any {
	return mock.MatchedBy(func(any) bool {
		return true
	})
}
