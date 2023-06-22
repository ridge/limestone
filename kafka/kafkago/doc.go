// Package kafkago contains the implementation of kafka.Client from kafka-go
// that works with real Kafka servers.
//
// Normally, outside lib/kafka, you shouldn't import this package directly. Most
// applications should use DefaultClient from lib/kafka to decide automatically
// which client to use.
package kafkago
