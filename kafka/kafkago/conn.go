package kafkago

import (
	"context"
	"math/rand" // choosing a random member of Kafka replicaset - not security-sensitive

	"github.com/ridge/limestone/tlog"
	"go.uber.org/zap"
)

func (c *client) shuffledBrokers() []string {
	brokers := make([]string, len(c.brokers), len(c.brokers))
	copy(brokers, c.brokers)
	rand.Shuffle(len(brokers), func(i, j int) {
		brokers[i], brokers[j] = brokers[j], brokers[i]
	})
	return brokers
}

func (c *client) getKafkaConn(ctx context.Context, topic string) (kafkaConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn := c.conn[topic]; conn != nil {
		return conn, nil
	}

	logger := tlog.Get(ctx).With(zap.String("topic", topic))

	var conn kafkaConn
	var err error
	for _, address := range c.shuffledBrokers() {
		conn, err = c.kafkaAPI.DialLeader(ctx, "tcp", address, topic, 0)
		if err == nil {
			c.conn[topic] = conn
			logger.Debug("Connected to Kafka for writing", zap.String("address", address))
			break
		}
		logger.Warn("Failed to connect to Kafka for writing", zap.Error(err), zap.String("address", address))
	}
	return conn, err
}

func (c *client) connectMaster(ctx context.Context) (kafkaConn, error) {
	logger := tlog.Get(ctx)

	var conn kafkaConn
	var err error
	for _, address := range c.shuffledBrokers() {
		conn, err = c.kafkaAPI.DialContext(ctx, "tcp", address)
		if err == nil {
			logger.Info("Connected to Kafka", zap.String("address", address))
			return conn, nil
		}
		logger.Warn("Failed to connect to Kafka", zap.Error(err), zap.String("address", address))
	}
	return conn, err
}

func (c *client) invalidateKafkaConn(topic string, conn kafkaConn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn[topic] == conn {
		delete(c.conn, topic)
	}
}
