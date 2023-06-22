package kafkacopy

// This package avoids importing lib/kafka to prevent CLI flag --kafka from
// being added
import (
	"context"
	"fmt"

	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/isempty"
	"github.com/ridge/limestone/kafka/names"
	"github.com/ridge/limestone/run"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const batchSize = 1000

// Config describes the copy tool configuration
type Config struct {
	From, To api.Client
	Topics   []string
	Rename   string
}

// Main handles the command line and runs the copy tool
func Main(args []string) {
	var from, to string
	var cfg Config
	pflag.StringVar(&from, "from", "", "Source Kafka URI")
	pflag.StringVar(&to, "to", "", "Destination Kafka URI")
	pflag.StringArrayVar(&cfg.Topics, "topic", nil, "Topic to copy (can be repeated). Default: copy all topics")
	pflag.StringVar(&cfg.Rename, "rename", "%s", "The format string to transform the topic names")
	_ = pflag.CommandLine.Parse(args[1:])

	cfg.From = kafkaClientFromURI("from", from)
	cfg.To = kafkaClientFromURI("to", to)

	run.Tool(func(ctx context.Context) error {
		return Run(ctx, cfg)
	})
}

func kafkaClientFromURI(label string, kafkaURI string) api.Client {
	if kafkaURI == "" {
		panic(fmt.Errorf("--%s is required", label))
	}
	return must.OK1(kafka.FromURI(kafkaURI))
}

// Run copies data from one Kafka instance to another
func Run(ctx context.Context, config Config) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		topics := config.Topics
		if len(topics) == 0 {
			var err error
			topics, err = config.From.Topics(ctx)
			if err != nil {
				return err
			}
		}
		tlog.Get(ctx).Info("Preparing to copy topics", zap.Strings("topics", topics))
		for _, topic := range topics {
			name := fmt.Sprintf(config.Rename, topic)
			if err := names.ValidateTopicName(name); err != nil {
				return err
			}
			empty, err := isempty.TopicIsEmpty(ctx, config.To, name)
			if err != nil {
				return err
			}
			if !empty {
				return fmt.Errorf("destination topic %s is not empty", name)
			}
		}
		for _, topic := range topics {
			topic := topic
			spawn(topic, parallel.Continue, func(ctx context.Context) error {
				return copyTopic(ctx, config.From, topic, config.To, fmt.Sprintf(config.Rename, topic))
			})
		}
		return nil
	})
}

func copyTopic(ctx context.Context, client1 api.Client, topic1 string, client2 api.Client, topic2 string) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		logger := tlog.Get(ctx).With(zap.String("fromTopic", topic1), zap.String("toTopic", topic2))
		logger.Info("Started copying")
		messages := make(chan *api.IncomingMessage, batchSize)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return client1.Read(ctx, topic1, 0, messages)
		})
		spawn("writer", parallel.Exit, func(ctx context.Context) error {
			write := func(ctx context.Context, topic string, messages []api.IncomingMessage) error {
				batch := make([]api.Message, 0, len(messages))
				for _, m := range messages {
					batch = append(batch, m.Message)
				}
				return client2.Write(ctx, topic, batch)
			}
			if client2backdated, ok := client2.(api.ClientBackdate); ok {
				write = client2backdated.WriteBackdated
			}
			var n int64
			batch := make([]api.IncomingMessage, 0, batchSize)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg := <-messages:
					// batch size reached or hot end encountered
					if len(batch) >= batchSize || msg == nil && len(batch) != 0 {
						if err := write(ctx, topic2, batch); err != nil {
							return fmt.Errorf("failed to copy topic %s to %s: %w", topic1, topic2, err)
						}
						batch = batch[:0] // truncate while keeping the underlying capacity
					}

					if msg == nil {
						logger.Info("Finished copying", zap.Int64("messagesCopied", n))
						return nil
					}

					// pad with blank messages to maintain offset parity
					for n < msg.Offset {
						batch = append(batch, api.IncomingMessage{
							Message: api.Message{Topic: topic2},
							Time:    msg.Time,
						})
						n++
					}

					m := *msg
					m.Topic = topic2
					batch = append(batch, m)
					n++
				}
			}
		})
		return nil
	})
}
