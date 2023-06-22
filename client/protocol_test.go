package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/gorilla/mux"
	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/limestone/tws"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
	"time"
)

type protocolTestEnv struct {
	group *parallel.Group

	client Client
	conns  int

	conn <-chan wire.Request
	up   <-chan wire.Transaction
	down chan<- *wire.IncomingTransaction
	drop chan<- struct{}
}

func protocolTestSetup(t *testing.T) *protocolTestEnv {
	var env protocolTestEnv

	env.group = test.Group(t)

	conn := make(chan wire.Request, 1)
	up := make(chan wire.Transaction, 1)
	down := make(chan *wire.IncomingTransaction, 1)
	drop := make(chan struct{}, 1)

	router := mux.NewRouter()
	router.HandleFunc("/pull", func(w http.ResponseWriter, r *http.Request) {
		tws.Serve(w, r, tws.StreamerConfig, func(ctx context.Context, incoming <-chan tws.Message, outgoing chan<- tws.Message) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-incoming:
				var req wire.Request
				must.OK(json.Unmarshal(msg.Data, &req))
				conn <- req
				if req.Version != 1 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case outgoing <- tws.Message{Data: must.OK1(json.Marshal(wire.Notification{Err: wire.ErrVersionMismatch(req.Version, 1)}))}:
						return nil
					}
				}
			}
			for {
				var notification wire.Notification
				select {
				case <-ctx.Done():
					return ctx.Err()
				case notification.Txn = <-down:
					if notification.Txn == nil {
						notification.Hot = true
					}
				case <-drop:
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case outgoing <- tws.Message{Data: must.OK1(json.Marshal(notification))}:
				}
			}
		})
	})
	router.HandleFunc("/push", func(w http.ResponseWriter, r *http.Request) {
		expected, _ := strconv.Atoi(r.URL.Query().Get("version"))
		if expected != 1 {
			http.Error(w, string(wire.ErrVersionMismatch(expected, 1)), http.StatusConflict)
			return
		}
		var txn wire.Transaction
		must.OK(json.Unmarshal(must.OK1(io.ReadAll(r.Body)), &txn))
		up <- txn
		w.WriteHeader(http.StatusNoContent)
	})

	server := thttp.NewServer(tnet.ListenOnRandomPort(), thttp.StandardMiddleware(router))
	env.client = newProtocolClient(server.ListenAddr().String(), time.Millisecond)
	env.group.Spawn("http", parallel.Fail, server.Run)

	env.conn = conn
	env.up = up
	env.down = down
	env.drop = drop

	return &env
}

func (env *protocolTestEnv) spawnConnection(version int, pos wire.Position, filter wire.Filter) (Connection, <-chan *wire.IncomingTransaction) {
	incoming := make(chan *wire.IncomingTransaction)
	conn := env.client.Connect(version, pos, filter, false)
	env.conns++
	env.group.Spawn(fmt.Sprintf("conn%d", env.conns), parallel.Fail, func(ctx context.Context) error {
		return conn.Run(ctx, incoming)
	})
	return conn, incoming
}

func (env *protocolTestEnv) spawnConnectionFail(version int, pos wire.Position, filter wire.Filter) (Connection, <-chan *wire.IncomingTransaction, <-chan error) {
	res := make(chan error)
	incoming := make(chan *wire.IncomingTransaction)
	conn := env.client.Connect(version, pos, filter, false)
	env.conns++
	env.group.Spawn(fmt.Sprintf("conn%d", env.conns), parallel.Continue, func(ctx context.Context) error {
		res <- conn.Run(ctx, incoming)
		return nil
	})
	return conn, incoming, res
}

func TestProtocolPull(t *testing.T) {
	env := protocolTestSetup(t)

	_, incoming := env.spawnConnection(1, wire.Position("0000000000000001-0000000000000002-0000000000000003"), wire.Filter{"apple": []string{"Color"}})
	require.Equal(t, wire.Request{Version: 1, Last: wire.Position("0000000000000001-0000000000000002-0000000000000003"), Filter: wire.Filter{"apple": []string{"Color"}}}, <-env.conn)

	env.down <- &wire.IncomingTransaction{Transaction: testTxn1, Position: wire.Position("0000000000000001-0000000000000002-0000000000000004")}
	require.Equal(t, &wire.IncomingTransaction{Transaction: testTxn1, Position: wire.Position("0000000000000001-0000000000000002-0000000000000004")}, <-incoming)
	env.down <- nil
	require.Nil(t, <-incoming)
}

func TestProtocolPush(t *testing.T) {
	env := protocolTestSetup(t)

	conn, _ := env.spawnConnection(1, wire.Beginning, nil)
	require.Equal(t, wire.Request{Version: 1}, <-env.conn)

	require.NoError(t, conn.Submit(env.group.Context(), testTxn1))
	require.Equal(t, testTxn1, <-env.up)
}

func TestProtocolPushMismatch(t *testing.T) {
	env := protocolTestSetup(t)

	conn, _, res := env.spawnConnectionFail(2, wire.Beginning, nil)
	require.Equal(t, wire.Request{Version: 2}, <-env.conn)

	require.Error(t, <-res, wire.ErrVersionMismatch(2, 1).Error())
	require.EqualError(t, conn.Submit(env.group.Context(), testTxn1), wire.ErrVersionMismatch(2, 1).Error())
}

func TestProtocolRetry(t *testing.T) {
	env := protocolTestSetup(t)

	_, incoming := env.spawnConnection(1, wire.Beginning, nil)
	require.Equal(t, wire.Request{Version: 1}, <-env.conn)

	env.down <- &wire.IncomingTransaction{Transaction: testTxn1, Position: wire.Position("0000000000000000-0000000000000000-0000000000000000")}
	require.Equal(t, &wire.IncomingTransaction{Transaction: testTxn1, Position: wire.Position("0000000000000000-0000000000000000-0000000000000000")}, <-incoming)
	env.down <- nil
	require.Nil(t, <-incoming)

	env.drop <- struct{}{}
	require.Equal(t, wire.Request{Version: 1, Last: wire.Position("0000000000000000-0000000000000000-0000000000000000")}, <-env.conn)

	env.down <- &wire.IncomingTransaction{Transaction: testTxn2, Position: wire.Position("0000000000000000-0000000000000000-0000000000000001")}
	require.Equal(t, &wire.IncomingTransaction{Transaction: testTxn2, Position: wire.Position("0000000000000000-0000000000000000-0000000000000001")}, <-incoming)
	env.down <- nil
	require.Nil(t, <-incoming)
}
