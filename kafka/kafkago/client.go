package kafkago

import (
	"sync"

	"github.com/ridge/limestone/kafka/api"
)

type client struct {
	brokers  []string
	kafkaAPI kafkaAPI

	conn map[string]kafkaConn // by topic
	mu   sync.Mutex
}

// New creates a real Kafka client that uses the specified brokers
func New(brokers []string) api.Client {
	return newClient(brokers, realKafkaAPI{})
}

func newClient(brokers []string, kafkaAPI kafkaAPI) api.Client {
	if len(brokers) == 0 {
		panic("need at least one Kafka broker")
	}
	return &client{
		brokers:  brokers,
		kafkaAPI: kafkaAPI,
		conn:     map[string]kafkaConn{},
	}
}
