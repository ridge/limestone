package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/wire"
	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/tws"
	"time"
)

func (c client) Read(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) error {
	url := "ws" + c.origin[4:] + "/kafka/" + topic + "?" + url.Values{
		"from": {strconv.FormatInt(offset, 10)},
	}.Encode()
	return retry.Do(ctx, retry.FixedConfig{RetryAfter: 5 * time.Second}, func() error {
		return retry.Retriable(tws.Dial(ctx, url, nil, tws.StreamerConfig, func(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case msg := <-incoming:
					if len(msg.Data) == 0 {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case dest <- nil:
						}
						continue
					}

					headerLen := bytes.IndexByte(msg.Data, '\n')
					if headerLen < 0 || msg.Data[len(msg.Data)-1] != '\n' {
						return errors.New("failed to read from remote Kafka server: message format error")
					}

					var header wire.Header
					if err := json.Unmarshal(msg.Data[:headerLen], &header); err != nil {
						return fmt.Errorf("failed to read from remote Kafka server: %w", err)
					}

					body := msg.Data[headerLen+1 : len(msg.Data)-1]
					if len(body) != header.Len {
						return errors.New("failed to read from remote Kafka server: message format error")
					}

					if header.Offset != nil {
						offset = *header.Offset
					}
					im := &api.IncomingMessage{
						Message: api.Message{
							Topic:   topic,
							Key:     header.Key,
							Headers: header.Headers,
							Value:   body,
						},
						Time:   header.TS,
						Offset: offset,
					}
					offset++

					select {
					case <-ctx.Done():
						return ctx.Err()
					case dest <- im:
					}
				}
			}
		}))
	})
}
