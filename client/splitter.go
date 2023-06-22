package client

import (
	"context"
	"sync"

	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
)

// A Splitter is a Client that feeds several connections from a single upstream
// connection. Its purpose is to optimize the case when several local Limestone
// instances need to consume the same transaction log.
//
// The Splitter behaves like a normal Client from which several connections can
// be created. However, Splitter requires the Run method to be called. New
// connections cannot be created after Run has started.
//
// Currently it's illegal to try to create several connections at different
// starting positions. There will be only one upstream reading position from
// which all connections of the splitter will be fed.
type Splitter struct {
	client Client

	mu         sync.Mutex
	ready      chan struct{}
	upstream   Connection
	downstream []*splitterConnection
	version    int
	startPos   wire.Position
	filter     wire.Filter
	compact    bool
}

type splitterConnection struct {
	splitter *Splitter
	ch       chan *wire.IncomingTransaction
}

// NewSplitter creates a new Splitter
func NewSplitter(upstream Client) *Splitter {
	return &Splitter{
		client: upstream,
		ready:  make(chan struct{}),
	}
}

// Connect creates a new local connection
func (s *Splitter) Connect(version int, pos wire.Position, filter wire.Filter, compact bool) Connection {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.upstream != nil {
		panic("cannot connect to client.Splitter once it's running")
	}

	if len(s.downstream) == 0 {
		s.version = version
		s.startPos = pos
		s.filter = filter
		s.compact = compact
	} else {
		if s.version != version {
			panic("all database versions for a client.Splitter must be the same")
		}
		if s.startPos != pos {
			panic("all starting positions for a client.Splitter must be the same")
		}
		if s.compact != compact {
			panic("all compact settings for a client.Splitter must be the same")
		}
		filterUnion(&s.filter, filter)
	}

	conn := &splitterConnection{splitter: s, ch: make(chan *wire.IncomingTransaction)}
	s.downstream = append(s.downstream, conn)
	return conn
}

// Run runs the splitter
func (s *Splitter) Run(ctx context.Context) error {
	s.connect()

	incoming := make(chan *wire.IncomingTransaction)
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("upstream", parallel.Fail, func(ctx context.Context) error {
			return s.upstream.Run(ctx, incoming)
		})
		spawn("split", parallel.Fail, func(ctx context.Context) error {
			var txn *wire.IncomingTransaction
			var ok bool
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case txn, ok = <-incoming:
					if !ok {
						return ctx.Err()
					}
					if err := s.deliver(ctx, txn); err != nil {
						return err
					}
				}
			}
		})
		return nil
	})
}

func (s *Splitter) connect() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.upstream != nil {
		panic("client.Splitter.Run may be called only once")
	}
	s.upstream = s.client.Connect(s.version, s.startPos, s.filter, s.compact)
	close(s.ready)
}

func (s *Splitter) deliver(ctx context.Context, txn *wire.IncomingTransaction) error {
	for _, conn := range s.downstream {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case conn.ch <- txn:
		}
	}
	return nil
}

func (sc *splitterConnection) Submit(ctx context.Context, txn wire.Transaction) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.splitter.ready:
		return sc.splitter.upstream.Submit(ctx, txn)
	}
}

func (sc *splitterConnection) Run(ctx context.Context, sink chan<- *wire.IncomingTransaction) error {
	for {
		var txn *wire.IncomingTransaction
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txn = <-sc.ch:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sink <- txn:
		}
	}
}

// modifies first argument
func filterUnion(a *wire.Filter, b wire.Filter) {
	if *a == nil {
		return
	}
	if b == nil {
		*a = nil
		return
	}
	for kind, props2 := range b {
		if props2 == nil {
			(*a)[kind] = nil
			continue
		}
		if props1, ok := (*a)[kind]; ok {
			if props1 == nil {
				continue
			}
			for _, p2 := range props2 {
				found := false
				for _, p1 := range props1 {
					if p1 == p2 {
						found = true
						break
					}
				}
				if !found {
					props1 = append(props1, p2)
				}
			}
			(*a)[kind] = props1
		} else {
			(*a)[kind] = props2
		}
	}
}
