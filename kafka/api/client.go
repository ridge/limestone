package api

import (
	"context"
	"errors"
)

// Client is a Kafka client
type Client interface {
	// Topics retrieves the list of topics.
	//
	// Keeps retrying on temporary errors, returns permanent ones.
	Topics(ctx context.Context) ([]string, error)

	// LastOffset retrieves the offset just after the last message in the topic,
	// or 0 if the topic is empty or does not exist.
	//
	// Keeps retrying on temporary errors, returns permanent ones.
	LastOffset(ctx context.Context, topic string) (int64, error)

	// Read reads messages from a Kafka topic starting from the given offset
	// and delivers them to the channel until the context is cancelled. Every
	// time reading reaches the end of stream, including at the beginning if the
	// topic is empty, nil is sent to the channel.
	//
	// Keeps retrying on temporary errors, returns permanent ones. Returns
	// ErrContinuityBroken if the topic is deleted and recreated while reading
	// is underway.
	Read(ctx context.Context, topic string, offset int64, dest chan<- *IncomingMessage) error

	// Write writes a batch of messages to a Kafka topic. All messages must be
	// destined for the specified topic.
	//
	// Either all messages are posted or none. Keeps retrying on temporary
	// errors, returns permanent ones.
	Write(ctx context.Context, topic string, messages []Message) error
}

// ClientBackdate is an extended interface implemented by some Kafka clients
type ClientBackdate interface {
	Client

	// WriteBackdated is similar to Write but saves the messages with the
	// specified timestamps instead of the current time. It is the caller's
	// responsibility to ensure nondescending order.
	WriteBackdated(ctx context.Context, topic string, messages []IncomingMessage) error
}

// ErrContinuityBroken is returned by Client.Read when it notices that the topic
// has been deleted from the server during reading.
//
// This error is returned on a best effort basis. Its detection is not perfect;
// it is possible for the reader to not notice it and keep running with
// undefined behavior.
var ErrContinuityBroken = errors.New("continuity broken")
