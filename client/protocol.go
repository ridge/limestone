package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/tws"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
	"time"
)

const retryInterval = 5 * time.Second

type protocolClient struct {
	server        string
	retryInterval time.Duration
}

// New creates a new server-based Limestone client for the given server:port
func New(server string) Client {
	return newProtocolClient(server, retryInterval)
}

func newProtocolClient(server string, retryInterval time.Duration) protocolClient {
	return protocolClient{
		server:        server,
		retryInterval: retryInterval,
	}
}

type protocolConnection struct {
	client     protocolClient
	version    int
	pos        wire.Position
	filter     wire.Filter
	compact    bool
	httpClient *http.Client
	sink       chan<- *wire.IncomingTransaction
	ready      chan struct{}
}

func (pc protocolClient) Connect(version int, pos wire.Position, filter wire.Filter, compact bool) Connection {
	return &protocolConnection{
		client:  pc,
		version: version,
		pos:     pos,
		filter:  filter,
		compact: compact,
		ready:   make(chan struct{}),
	}
}

func (pc *protocolConnection) Run(ctx context.Context, sink chan<- *wire.IncomingTransaction) error {
	logger := tlog.Get(ctx)

	pc.httpClient = thttp.WithRequestsLogging(&http.Client{})
	pc.sink = sink
	close(pc.ready)
	url := fmt.Sprintf("ws://%s/pull", pc.client.server)
	for {
		logger.Debug("Trying to connect to Limestone server", zap.String("url", url))
		err := tws.Dial(ctx, url, nil, tws.StreamerConfig, pc.session)
		if err != nil && !errors.Is(err, ctx.Err()) {
			logger.Debug("Connection to Limestone server failed", zap.String("url", url), zap.Error(err))
		}
		var mismatch wire.ErrMismatch
		if errors.As(err, &mismatch) {
			return mismatch
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pc.client.retryInterval):
		}
	}
}

func (pc *protocolConnection) session(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) (err error) {
	logger := tlog.Get(ctx)

	req := wire.Request{Version: pc.version, Last: pc.pos, Filter: pc.filter, Compact: pc.compact}
	// FIXME (alexey): request field temporarily renamed to limestoneRequest to
	// work around Elasticsearch restriction that the same field name cannot be
	// used for a string value in one message and for an object in any other
	// message. Revert when it becomes possible.
	logger.Info("Connected to Limestone server", zap.Any("limestoneRequest", req))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outgoing <- tws.Message{Data: must.OK1(json.Marshal(req))}:
	}

	// The hot start sequence is non-interruptible: hot start messages don't
	// have a position which is used as a restart token. Restarting without a
	// restart token can lead to local data corruption.
	//
	// An interruption during hot start causes a soft restart: the service exits
	// and restarts without logging a panic. This should be very unlikely
	// because in the normal order in which services are restarted, the
	// limestone server restarts first.
	restartable := true
	defer func() {
		var mismatch wire.ErrMismatch
		if !restartable && !errors.As(err, &mismatch) {
			if err == nil {
				err = wire.ErrMismatch("hot start interrupted")
			} else {
				err = wire.ErrMismatch("hot start interrupted: " + err.Error())
			}
		}
	}()

	for {
		var notification wire.Notification
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-incoming:
			if !ok {
				return nil
			}
			if err := json.Unmarshal(msg.Data, &notification); err != nil {
				return fmt.Errorf("failed to parse incoming WS message: %w", err)
			}
		}

		if notification.Err != "" {
			return notification.Err
		}

		if notification.Txn != nil {
			pc.pos = notification.Txn.Position
			restartable = pc.pos != "" // cannot restart without a nonempty position
			select {
			case <-ctx.Done():
				return ctx.Err()
			case pc.sink <- notification.Txn:
			}
		}

		if notification.Hot {
			restartable = true
			select {
			case <-ctx.Done():
				return ctx.Err()
			case pc.sink <- nil:
			}
		}
	}
}

func (pc *protocolConnection) Submit(ctx context.Context, txn wire.Transaction) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pc.ready:
	}

	url := fmt.Sprintf("http://%s/push?version=%d", pc.client.server, pc.version)
	body := must.OK1(json.Marshal(txn))
	ctx = tlog.With(ctx, zap.String("url", url))

	return retry.Do(ctx, retry.FixedConfig{RetryAfter: pc.client.retryInterval}, func() error {
		req := must.OK1(http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body)))
		res, err := pc.httpClient.Do(req)
		if err != nil {
			return retry.Retriable(err)
		}
		defer res.Body.Close()

		switch res.StatusCode {
		case http.StatusNoContent:
			return nil
		case http.StatusConflict, http.StatusServiceUnavailable:
			b, err := io.ReadAll(res.Body)
			if err != nil {
				return retry.Retriable(fmt.Errorf("failed to read error response: %w", err))
			}
			errText := strings.TrimSpace(string(b))
			if errText != "" {
				err := wire.ErrMismatch(errText)
				if res.StatusCode == http.StatusServiceUnavailable {
					// When the server's version is behind ours, the server
					// reports it as a retriable 5xx error. This can happen
					// during an upgrade, so we should retry and expect the new
					// Limestone server to respond.
					return retry.Retriable(err)
				}
				return err
			}
			fallthrough
		default:
			return retry.Retriable(fmt.Errorf("%s returned status code %d", url, res.StatusCode))
		}
	})
}
