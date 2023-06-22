package mock

import (
	"sync"

	"github.com/ridge/limestone/kafka/api"
	"time"
)

type message struct {
	ts      time.Time
	key     string
	headers map[string]string
	value   []byte
}

type topic struct {
	data []message
	more chan struct{}
}

type kafka struct {
	mu     sync.RWMutex
	topics map[string]*topic
	more   chan struct{}
}

// New creates a new in-memory Kafka simulator
func New() api.ClientBackdate {
	return &kafka{
		topics: map[string]*topic{},
		more:   make(chan struct{}),
	}
}
