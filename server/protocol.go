package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/tws"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
)

func (s server) pull(w http.ResponseWriter, r *http.Request) {
	tws.Serve(w, r, tws.StreamerConfig, func(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) error {
		logger := tlog.Get(r.Context())

		// 1. Read the request
		var req wire.Request
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-incoming:
			if !ok {
				return nil
			}
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				return err
			}
		}
		// FIXME (alexey): request field temporarily renamed to limestoneRequest to
		// work around Elasticsearch restriction that the same field name cannot be
		// used for a string value in one message and for an object in any other
		// message. Revert when it becomes possible.
		logger.Info("Client connected", zap.Any("limestoneRequest", req))
		defer logger.Info("Client disconnected")

		// 2. Connect to Kafka
		pos := req.Last
		if req.Compact && req.Last == wire.Beginning {
			pos = s.hotStartPos
		}
		conn := s.upstream.Connect(req.Version, pos, req.Filter, req.Compact)

		filter := compileFilter(req.Filter)
		txns := make(chan *wire.IncomingTransaction)
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("client", parallel.Continue, func(ctx context.Context) error {
				err := conn.Run(ctx, txns)
				var mismatch wire.ErrMismatch
				if errors.As(err, &mismatch) {
					select {
					case <-ctx.Done():
						return err
					case outgoing <- tws.Message{Data: must.OK1(json.Marshal(wire.Notification{Err: mismatch}))}:
						return nil
					}
				}
				return err
			})

			// 3. Push hot start data, then handle kafka updates
			spawn("down", parallel.Fail, func(ctx context.Context) error {
				if req.Compact && req.Version == s.version && req.Last == wire.Beginning {
					filtered := 0
					for kind, messages := range s.hotStart {
						if _, ok := req.Filter[kind]; ok || req.Filter == nil {
							for _, msg := range messages {
								select {
								case <-ctx.Done():
									return ctx.Err()
								case outgoing <- tws.Message{Data: msg}:
									filtered++
								}
							}
						}
					}
					if filtered != 0 {
						logger.Debug("Sent hot start data", zap.Int("filtered", filtered))
					}
				}

				hot := false
				unfiltered := 0
				filtered := 0
				for {
					var notification wire.Notification
					select {
					case <-ctx.Done():
						return ctx.Err()
					case txn := <-txns:
						if txn == nil {
							if hot {
								continue
							}
							logger.Debug("Filtered a batch of transactions and reached hot end", zap.Int("unfiltered", unfiltered), zap.Int("filtered", filtered))
							unfiltered = 0
							filtered = 0
							notification.Hot = true
						} else {
							unfiltered++
							txn.Changes = filter(txn.Changes)
							if txn.Changes == nil {
								continue
							}
							filtered++
							notification.Txn = txn
						}
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case outgoing <- tws.Message{Data: must.OK1(json.Marshal(notification))}:
					}

					hot = notification.Hot
				}
			})

			// 4. Watch the incoming stream
			spawn("up", parallel.Exit, func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case _, ok := <-incoming:
					if ok {
						return errors.New("unexpected message from client")
					}
					return nil
				}
			})

			return nil
		})
	})
}

func (s server) push(w http.ResponseWriter, r *http.Request) {
	logger := tlog.Get(r.Context())

	version, err := strconv.Atoi(r.URL.Query().Get("version"))
	if err != nil {
		http.Error(w, "failed to parse required query parameter: version", http.StatusBadRequest)
		return
	}
	if version != s.version {
		status := http.StatusConflict
		if version > s.version {
			// If the requested version is ahead of ours, report it as a
			// retriable 5xx error. This can happen during an upgrade, so the
			// client should retry and expect the new Limestone server to
			// respond.
			status = http.StatusServiceUnavailable
		}
		http.Error(w, wire.ErrVersionMismatch(version, s.version).Error(), status)
		return
	}

	var txn wire.Transaction
	must.OK(json.Unmarshal(must.OK1(io.ReadAll(r.Body)), &txn))
	logger.Debug("Submitting transaction", zap.Object("txn", txn))
	if err := s.master.Submit(r.Context(), txn); err != nil {
		logger.Error("Failed to submit transaction", zap.Error(err))
		http.Error(w, "failed to submit transaction", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
