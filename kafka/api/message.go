package api

import (
	"go.uber.org/zap/zapcore"
	"time"
)

// Message is an outgoing Kafka message
type Message struct {
	Topic, Key string
	Headers    map[string]string
	Value      []byte
}

// IncomingMessage is an incoming Kafka message
type IncomingMessage struct {
	Message
	Time   time.Time
	Offset int64
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Message with zap.Object
func (msg Message) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("key", msg.Key)
	e.AddString("topic", msg.Topic)
	err := e.AddObject("headers", zapcore.ObjectMarshalerFunc(func(e zapcore.ObjectEncoder) error {
		for k, v := range msg.Headers {
			e.AddString(k, v)
		}
		return nil
	}))
	if err != nil {
		return err
	}
	e.AddByteString("value", msg.Value)
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of IncomingMessage with zap.Object
func (msg IncomingMessage) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if err := msg.Message.MarshalLogObject(e); err != nil {
		return err
	}
	e.AddTime("time", msg.Time)
	e.AddInt64("offset", msg.Offset)
	return nil
}
