package tws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

	"time"

	"github.com/gorilla/websocket"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
)

// Config is the WebSocket configuration
type Config struct {
	// Timeout for the WebSocket protocol upgrade
	HandshakeTimeout time.Duration

	// Disconnect when an outgoing packet is not acknowledged for this long.
	// 0 for kernel default.
	TCPTimeout time.Duration

	// Send pings this often. 0 to disable.
	PingInterval time.Duration

	// Disconnect if a pong doesn't arrive during PingInterval
	RequirePong bool

	// Request specific Websocket subprotocols. Client-only.
	Subprotocols []string

	// Pass specific TLS configuration to the connection. Client-only.
	TLSClientConfig *tls.Config

	// CheckOrigin returns true if the request Origin header is acceptable
	CheckOrigin func(r *http.Request) bool
}

// DefaultConfig is the default Config value
var DefaultConfig = Config{
	HandshakeTimeout: 5 * time.Second,

	TCPTimeout: 30 * time.Second,

	PingInterval: 30 * time.Second,
	RequirePong:  true,
}

// StreamerConfig is the recommended configuration for high-traffic protocols
// where participants cannot be expected to be responsive all the time
var StreamerConfig = func() Config {
	config := DefaultConfig
	config.RequirePong = false
	return config
}()

// SessionFn is a function that implements a WebSocket interaction scenario.
//
// The function receives incoming messages through one channel and sends outgoing messages through another.
// Both the incoming channel and the context will be closed when the connection closes.
// Once the session function returns, the connection will be closed if it's still open.
// If the session function returns nil, the closure will be graceul: the incoming channel will be drained first.
// If the session function returns an error, the connection will be closed immediately.
type SessionFn func(ctx context.Context, incoming <-chan Message, outgoing chan<- Message) error

// Serve handles an HTTP request by upgrading the connection to WebSocket
// and executing the interaction scenario described by the session function.
//
// The context passed into the session function is a descendant of the request context.
func Serve(w http.ResponseWriter, r *http.Request, config Config, sessionFn SessionFn) {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: config.HandshakeTimeout,
		CheckOrigin:      config.CheckOrigin,
	}
	logger := tlog.Get(r.Context())

	// Copying w.Header gets us, in particular, X-Ridge-Request-ID set by
	// thttp.Log.
	ws, err := upgrader.Upgrade(w, r, w.Header().Clone())
	if err != nil {
		logger.Error("Failed to serve WebSocket connection", zap.Error(err))
		return
	}

	if err := tuneTCP(ws.UnderlyingConn(), config); err != nil {
		ws.Close()
		logger.Error("Failed to serve WebSocket connection", zap.Error(err))
		return
	}

	err = handleSession(r.Context(), ws, config, sessionFn)
	logger.Info("WebSocket disconnected", zap.Error(err))
}

// Dial connects to WebSocket server and executes the interaction scenario described by the session function.
func Dial(ctx context.Context, url string, headers http.Header, config Config, sessionFn SessionFn) error {
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var netDialer net.Dialer
			conn, err := netDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			err = tuneTCP(conn, config)
			if err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: config.HandshakeTimeout,
		Subprotocols:     config.Subprotocols,
		TLSClientConfig:  config.TLSClientConfig,
	}

	ws, resp, err := dialer.DialContext(ctx, url, headers)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
			return fmt.Errorf("failed to establish WebSocket connection to %s (%s): %w", url, resp.Status, err)
		}
		return fmt.Errorf("failed to establish WebSocket connection to %s: %w", url, err)
	}

	ctx = tlog.With(ctx, zap.String("url", url), zap.String("requestID", resp.Header.Get("X-Ridge-Request-ID")))
	return handleSession(ctx, ws, config, sessionFn)
}

// Message defines the message passed through WebSocket
type Message struct {
	Binary bool
	Data   []byte
}

func handleSession(ctx context.Context, ws *websocket.Conn, config Config, sessionFn SessionFn) error {
	logger := tlog.Get(ctx)
	logger.Info("WebSocket established")

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		var pings int64 // difference between pings sent and pongs received
		incoming := make(chan Message)
		outgoing := make(chan Message)

		if config.RequirePong {
			ws.SetPongHandler(func(data string) error {
				atomic.AddInt64(&pings, -1)
				return nil
			})
		}

		spawn("session", parallel.Continue, func(ctx context.Context) error {
			defer close(outgoing)
			return sessionFn(ctx, incoming, outgoing)
		})

		spawn("receiver", parallel.Continue, func(ctx context.Context) error {
			defer close(incoming)

			for {
				mt, buff, err := ws.ReadMessage()
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					var e *websocket.CloseError
					if errors.As(err, &e) {
						return nil
					}
					return err
				}
				switch mt {
				case websocket.TextMessage, websocket.BinaryMessage:
					select {
					case incoming <- Message{Binary: mt == websocket.BinaryMessage, Data: buff}:
					case <-ctx.Done():
						return ctx.Err()
					}
				default:
					return fmt.Errorf("unexpected WebSocket message type %d", mt)
				}
			}
		})

		spawn("sender", parallel.Exit, func(ctx context.Context) error {
			// We use websocket pings because Nginx closes the connection if no traffic passes it
			// despite configuring TCP keepalive.

			var ticks <-chan time.Time
			if config.PingInterval != 0 {
				ticker := time.NewTicker(config.PingInterval)
				defer ticker.Stop()
				ticks = ticker.C
			}
			for {
				// Keep in mind that gorilla websocket library does not support concurrent writes (WriteMessage, WriteControl...)
				// so sending real messages and pings have to happen in the same goroutine or be protected by mutex.
				// We have chosen the first solution here.
				select {
				case msg, ok := <-outgoing:
					if !ok {
						return nil
					}
					messageType := websocket.TextMessage
					if msg.Binary {
						messageType = websocket.BinaryMessage
					}
					if err := ws.WriteMessage(messageType, msg.Data); err != nil {
						return err
					}
				case <-ticks:
					if config.RequirePong && atomic.AddInt64(&pings, 1) > 1 { // we still haven't received the previous pong
						return errors.New("WebSocket ping timeout")
					}
					if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
						return err
					}
				}
			}
		})

		spawn("closer", parallel.Exit, func(ctx context.Context) error {
			<-ctx.Done()
			if err := ws.Close(); err != nil {
				// If the other side terminates WebSocket connection, then TLS
				// library sometimes returns this error due to unfortunate timing
				// of socket operations.
				//
				// Unfortunately, the error is produced by fmt.Errorf, so we have
				// to resort to checking the error text.
				if !strings.Contains(err.Error(), "failed to send closeNotify alert (but connection was closed anyway)") {
					return err
				}
			}

			return ctx.Err()
		})

		return nil
	})
}

// WithWSScheme changes http to ws and https to wss
func WithWSScheme(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		panic("no scheme in address")
	}
	return strings.Replace(addr, "http", "ws", 1)
}
