package thttp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/ridge/limestone/tcontext"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
	"time"
)

const gracefulShutdownTimeout = 5 * time.Second

// Server wraps an HTTP server
type Server struct {
	listener net.Listener
	handler  http.Handler
	locked   sync.WaitGroup
}

// NewServer creates a Server
func NewServer(listener net.Listener, handler http.Handler) *Server {
	return &Server{
		listener: listener,
		handler:  handler,
	}
}

type panicKeyType int

const panicKey panicKeyType = iota

// Run serves requests until the context is closed, then performs graceful
// shutdown for up to gracefulShutdownTimeout
func (s *Server) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		panicChan := make(chan error, 1)
		ctx = context.WithValue(ctx, panicKey, panicChan)
		ctx = tlog.With(ctx, zap.Stringer("httpServer", s.listener.Addr()))
		reqCtx, reqCancel := context.WithCancel(tcontext.Reopen(ctx)) // stays open longer than ctx

		logger := tlog.Get(ctx)

		server := http.Server{
			Handler:     s.handler,
			ErrorLog:    must.OK1(zap.NewStdLogAt(logger, zap.WarnLevel)),
			BaseContext: func(net.Listener) context.Context { return reqCtx },
			ConnContext: s.connContext,
		}
		server.Handler = s.lock(server.Handler) // install as outermost

		spawn("serve", parallel.Fail, func(ctx context.Context) error {
			logger.Info("Serving requests")
			err := server.Serve(s.listener)
			// http.Server predates contexts, so it has its own
			// error meaning "terminated successfully due to an
			// external request". Return the actual error from
			// Context in this case to avoid accidentally treating
			// successful shutdown as an error.
			if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		})

		spawn("panicHandler", parallel.Fail, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-panicChan:
				return err
			}
		})

		spawn("shutdownHandler", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			logger.Info("Shutting down")

			shutdownCtx, cancel := context.WithTimeout(reqCtx, gracefulShutdownTimeout)
			defer cancel()
			defer reqCancel()
			defer server.Close() // always returns nil because the listener is already closed

			// Server.Shutdown may return http.ErrServerClosed if
			// the server is already down. It's not an error in this
			// case.
			err := server.Shutdown(shutdownCtx)
			if err != nil {
				if shutdownCtx.Err() != nil { // timeout shutting down
					logger.Info("Shutdown canceled", zap.Error(err))
					return err
				}

				// All other errors come from closing listener, and we don't care
				// about them, as the server is shutting down anyway.
			}

			reqCancel() // ask hijacked connections to terminate
			s.locked.Wait()

			logger.Info("Shutdown complete")
			return ctx.Err()
		})

		return nil
	})
}

// ListenAddr returns the local address of the server's listener
func (s *Server) ListenAddr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) connContext(ctx context.Context, conn net.Conn) context.Context {
	return tlog.With(ctx, zap.Stringer("remoteAddr", conn.RemoteAddr()))
}

// This mandatory Middleware ensures that any running handlers prevent the
// server from shutting down. This is normally taken care of by the standard
// library itself, except when connections are hijacked. The latter use case is
// important for WebSocket.
func (s *Server) lock(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.locked.Add(1)
		defer s.locked.Done()
		next.ServeHTTP(w, r)
	})
}

// Wrap installs a number of middleware on HTTP handler. The first
// middleware listed will be the first one to see the request.
func Wrap(handler http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		handler = mw[i](handler)
	}
	return handler
}

// StandardMiddleware is a composition of typically used middleware, in the
// recommended order:
//
// 1. Log (log before and after the request)
// 2. Recover (catch and log panic, then shut down the server)
// 3. CORS (allow cross-origin requests)
func StandardMiddleware(next http.Handler) http.Handler {
	return Log(Recover(CORS(next)))
}
