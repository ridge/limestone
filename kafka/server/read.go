package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/wire"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/tws"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"go.uber.org/zap"
)

func (h handler) topicInfo(w http.ResponseWriter, r *http.Request) {
	offset, err := h.client.LastOffset(r.Context(), mux.Vars(r)["topic"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
		}
		return
	}
	w.Header().Set("X-Last-Offset", strconv.FormatInt(offset, 10))
	w.WriteHeader(http.StatusOK)
}

func (h handler) topic(w http.ResponseWriter, r *http.Request) {
	if strings.ToLower(r.Header.Get("Connection")) == "upgrade" {
		h.topicWS(w, r)
	} else {
		h.topicPlain(w, r)
	}
}

// Plain format: same as local kafka (can be written into a local-kafka file directly);
// stops at hot end or error.
//
// Unfortunately, there is no nice way to report an error. This handler sets an
// HTTP trailer (X-Error).
//
// ?from= specifies the starting offset; default: 0.
func (h handler) topicPlain(w http.ResponseWriter, r *http.Request) {
	var offset int64
	if from := r.URL.Query().Get("from"); from != "" {
		var err error
		offset, err = strconv.ParseInt(from, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(err.Error())); err != nil {
				tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
			}
			return
		}
	}
	w.Header().Set("Trailer", "X-Error")
	w.Header().Add("Vary", "Accept-Encoding")
	err := parallel.Run(r.Context(), func(ctx context.Context, spawn parallel.SpawnFn) error {
		messages := make(chan *api.IncomingMessage)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return h.client.Read(ctx, mux.Vars(r)["topic"], offset, messages)
		})
		spawn("writer", parallel.Exit, func(ctx context.Context) error {
			var buf bytes.Buffer
			var writer io.Writer
			if thttp.ShouldGzip(r) {
				gzw := gzip.NewWriter(w)
				defer gzw.Close()

				writer = gzw
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				writer = w
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg := <-messages:
					if msg == nil {
						return nil
					}
					buf.Reset()
					must.OK1(buf.Write(must.OK1(json.Marshal(wire.Header{
						TS:      msg.Time,
						Offset:  &msg.Offset,
						Key:     msg.Key,
						Headers: msg.Headers,
						Len:     len(msg.Value),
					}))))
					must.OK(buf.WriteByte('\n'))
					must.OK1(buf.Write(msg.Value))
					must.OK(buf.WriteByte('\n'))
					if _, err := buf.WriteTo(writer); err != nil {
						tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
						return nil
					}
				}
			}
		})
		return nil
	})
	if err != nil && !errors.Is(err, r.Context().Err()) {
		w.Header().Set("X-Error", err.Error())
	}
}

// WebSocket: individual WS messages are in the same format as local kafka;
// zero-length WS message means hot end; keeps tailing the topic until the
// client hangs up or an error occurs.
//
// ?from= specifies the starting offset; default: 0.
// ?from=tail means start at the hot end and watch for new records.
func (h handler) topicWS(w http.ResponseWriter, r *http.Request) {
	var offset *int64
	switch from := r.URL.Query().Get("from"); from {
	case "tail": // nil
	case "": // 0
		offset = new(int64)
	default:
		var err error
		offset = new(int64)
		*offset, err = strconv.ParseInt(from, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(err.Error())); err != nil {
				tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
			}
			return
		}
	}
	tws.Serve(w, r, tws.StreamerConfig, func(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) error {
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			if offset == nil {
				var err error
				offset = new(int64)
				*offset, err = h.client.LastOffset(r.Context(), mux.Vars(r)["topic"])
				if err != nil {
					return err
				}
			}
			messages := make(chan *api.IncomingMessage)
			spawn("reader", parallel.Fail, func(ctx context.Context) error {
				return h.client.Read(ctx, mux.Vars(r)["topic"], *offset, messages)
			})
			spawn("writer", parallel.Exit, func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case _, ok := <-incoming:
						if ok {
							return errors.New("unexpected incoming message")
						}
						return nil
					case msg := <-messages:
						var buf bytes.Buffer
						if msg != nil {
							must.OK1(buf.Write(must.OK1(json.Marshal(wire.Header{
								TS:      msg.Time,
								Offset:  &msg.Offset,
								Key:     msg.Key,
								Headers: msg.Headers,
								Len:     len(msg.Value),
							}))))
							must.OK(buf.WriteByte('\n'))
							must.OK1(buf.Write(msg.Value))
							must.OK(buf.WriteByte('\n'))
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case outgoing <- tws.Message{Data: buf.Bytes()}:
						}
					}
				}
			})
			return nil
		})
	})
}

// A single record in a topic, same format as local kafka
func (h handler) record(w http.ResponseWriter, r *http.Request) {
	offset, err := strconv.ParseInt(mux.Vars(r)["offset"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
		}
		return
	}
	last, err := h.client.LastOffset(r.Context(), mux.Vars(r)["topic"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
		}
		return
	}
	if offset < 0 || offset >= last {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = parallel.Run(r.Context(), func(ctx context.Context, spawn parallel.SpawnFn) error {
		messages := make(chan *api.IncomingMessage)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return h.client.Read(ctx, mux.Vars(r)["topic"], offset, messages)
		})
		spawn("writer", parallel.Exit, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-messages:
				var buf bytes.Buffer
				must.OK1(buf.Write(must.OK1(json.Marshal(wire.Header{
					TS:      msg.Time,
					Offset:  &msg.Offset,
					Key:     msg.Key,
					Headers: msg.Headers,
					Len:     len(msg.Value),
				}))))
				must.OK(buf.WriteByte('\n'))
				must.OK1(buf.Write(msg.Value))
				must.OK(buf.WriteByte('\n'))
				if _, err := buf.WriteTo(w); err != nil {
					tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
				}
				return nil
			}
		})
		return nil
	})
}
