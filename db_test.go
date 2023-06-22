package limestone

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"time"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/kafka/mock"
	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/require"
)

// Many of these tests rely on timeout for failure. They wait for a condition
// that becomes true almost immediately when the code is correct, but never if
// the test fails. This is uncomfortable for debugging, so this suite implements
// its own short timeouts.
//
// Upon timeout, the tap channels are closed, which causes the waiting code to
// give up and fail the test.
const testTimeout = time.Second

type fooID string
type fooA struct {
	Meta `limestone:"name=foo,producer=a"`
	ID   fooID `limestone:"identity"`
	A    int
}
type fooB struct {
	Meta `limestone:"name=foo,producer=b"`
	B    int
}
type foo struct {
	fooA
	fooB
}

func (f foo) Survive() bool {
	return f.B >= 0
}

type barID string
type barA struct {
	Meta  `limestone:"name=bar,producer=a"`
	ID    barID `limestone:"identity"`
	FooID fooID
	A     string
}
type barB struct {
	Meta `limestone:"name=bar,producer=b"`
	B    string
}
type bar struct {
	barA
	barB
}

var (
	kindFoo    = KindOf(foo{})
	indexFooID = indices.FieldIndex("FooID")
	kindBar    = KindOf(bar{}, indexFooID)
)

type option interface {
	apply(*Config)
}

type optTap chan<- Snapshot

func (o optTap) apply(c *Config) {
	c.DebugTap = o
}

type optWakeUp WakeUpFn

func (o optWakeUp) apply(c *Config) {
	c.WakeUp = WakeUpFn(o)
}

func testEnv(t *testing.T) (kafka.Client, *parallel.Group) {
	k := mock.New()
	group := test.GroupWithTimeout(t, testTimeout)
	return k, group
}

func createDB(kafka kafka.Client, group *parallel.Group, source Source, opt ...option) *DB {
	config := Config{
		Client:   client.NewKafkaClient(kafka),
		Entities: KindList{kindFoo, kindBar},
		Source:   source,
		Logger:   tlog.Get(group.Context()),
		Session:  1,
	}

	for _, o := range opt {
		o.apply(&config)
	}

	db := New(config)

	var suffix string
	if source.Producer != "" {
		suffix = ":" + string(source.Producer)
	}
	group.Spawn("limestone"+suffix, parallel.Fail, func(ctx context.Context) error {
		if config.DebugTap != nil {
			defer close(config.DebugTap)
		}
		return db.Run(ctx)
	})

	return db
}

func TestAdminBeforeStart(t *testing.T) {
	k, group := testEnv(t)

	require.NoError(t, Bootstrap(group.Context(), k, 0, "txlog",
		foo{fooA: fooA{ID: fooID("f1")}},
		foo{fooA: fooA{ID: fooID("f2")}, fooB: fooB{B: -1}},
	))

	tap := make(chan Snapshot)
	createDB(k, group, Source{}, optTap(tap))

	for snapshot := range tap {
		if snapshot.Get(fooID("f1"), new(foo)) {
			break
		}
		var f foo
		require.False(t, snapshot.Get(fooID("f2"), &f))
	}
	require.NoError(t, group.Context().Err()) // not timed out
}

func TestAdminAfterStart(t *testing.T) {
	k, group := testEnv(t)

	tap := make(chan Snapshot)
	createDB(k, group, Source{}, optTap(tap))

	require.NoError(t, Bootstrap(group.Context(), k, 0, "txlog",
		foo{fooA: fooA{ID: fooID("f1")}},
		foo{fooA: fooA{ID: fooID("f2")}, fooB: fooB{B: -1}},
	))

	for snapshot := range tap {
		if snapshot.Get(fooID("f1"), new(foo)) {
			break
		}
		var f foo
		require.False(t, snapshot.Get(fooID("f2"), &f))
	}
	require.NoError(t, group.Context().Err()) // not timed out
}

func TestAudit(t *testing.T) {
	k, group := testEnv(t)

	require.NoError(t, Bootstrap(group.Context(), k, 0, "txlog"))

	a := createDB(k, group, Source{Producer: "a"})
	require.NoError(t, a.WaitReady(group.Context()))

	var wtxn wire.Transaction
	messages := make(chan *kafka.IncomingMessage, 2)
	group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return k.Read(ctx, "txlog", 0, messages)
	})
	require.Nil(t, <-messages)

	a.Do(func(txn Transaction) {
		txn.Set(foo{fooA: fooA{ID: "f1"}})
	})
	require.NoError(t, json.Unmarshal((<-messages).Value, &wtxn))
	require.Nil(t, <-messages)
	require.Nil(t, wtxn.Audit)

	a.Do(func(txn Transaction) {
		txn.Set(foo{fooA: fooA{ID: "f2"}})
		txn.Annotate("animal", "squirrel")
		txn.Annotate("vegetable", "carrot")
		txn.Annotate("animal", "squirrel")
	})
	require.NoError(t, json.Unmarshal((<-messages).Value, &wtxn))
	require.Nil(t, <-messages)
	var audit map[string]string
	require.NoError(t, json.Unmarshal(wtxn.Audit, &audit))
	require.Equal(t, map[string]string{"animal": "squirrel", "vegetable": "carrot"}, audit)
}

func TestResponse(t *testing.T) {
	k, group := testEnv(t)
	require.NoError(t, Bootstrap(group.Context(), k, 0, "txlog"))

	tap := make(chan Snapshot, 1)
	a := createDB(k, group, Source{Producer: "a"}, optTap(tap))

	createDB(k, group, Source{Producer: "b"}, optWakeUp(func(ctx context.Context, txn Transaction, entities []any) {
		for _, e := range entities {
			switch e := e.(type) {
			case foo:
				e.B = e.A
				txn.Set(e)
			default:
				require.FailNow(t, "unexpected type %T", e)
			}
		}
	}))

	require.NoError(t, a.WaitReady(group.Context()))
	a.Do(func(txn Transaction) {
		txn.Set(foo{fooA: fooA{ID: fooID("f1"), A: 7}})
	})

	for snapshot := range tap {
		var f1 foo
		if !snapshot.Get(fooID("f1"), &f1) || f1.B == 0 {
			continue
		}
		require.Equal(t, 7, f1.B)
		break
	}
	require.NoError(t, group.Context().Err()) // not timed out

	a.Do(func(txn Transaction) {
		var f1 foo
		MustGet(txn, fooID("f1"), &f1)
		f1.A = 77
		txn.Set(f1)
	})

	for snapshot := range tap {
		var f1 foo
		MustGet(snapshot, fooID("f1"), &f1)
		if f1.B == 7 {
			continue
		}
		require.Equal(t, 77, f1.B)
		break
	}

	a.Do(func(txn Transaction) {
		var f1 foo
		MustGet(txn, fooID("f1"), &f1)
		f1.A = -7
		txn.Set(f1)
	})

	for snapshot := range tap {
		var f1 foo
		MustGet(snapshot, fooID("f1"), &f1)
		if f1.B == 77 {
			continue
		}
		require.Equal(t, -7, f1.B)
		break
	}

	a.Do(func(txn Transaction) {
		var f1 foo
		require.False(t, txn.Get(fooID("f1"), &f1))
	})

	require.NoError(t, group.Context().Err()) // not timed out
}

func TestResponseLinked(t *testing.T) {
	k, group := testEnv(t)

	require.NoError(t, Bootstrap(group.Context(), k, 0, "txlog"))

	tap := make(chan Snapshot, 1)
	a := createDB(k, group, Source{Producer: "a"}, optTap(tap))

	processFoo := func(txn Transaction, id fooID) {
		var parent foo
		var child bar

		MustGet(txn, id, &parent)
		parent.B = parent.A
		iter := txn.Search(kindBar, indexFooID, id)
		for iter(&child) {
			parent.B += len(child.A)
			child.B = child.A + strconv.Itoa(parent.A)
			txn.Set(child)
		}
		txn.Set(parent)
	}

	createDB(k, group, Source{Producer: "b"}, optWakeUp(func(ctx context.Context, txn Transaction, entities []any) {
		for _, e := range entities {
			switch e := e.(type) {
			case foo:
				processFoo(txn, e.ID)
			case bar:
				var old bar
				if txn.Before().Get(e.ID, &old) && old.FooID != e.FooID {
					processFoo(txn, old.FooID)
				}
				processFoo(txn, e.FooID)
			default:
				require.FailNow(t, "unexpected type %T", e)
			}
		}
	}))

	require.NoError(t, a.WaitReady(group.Context()))
	a.Do(func(txn Transaction) {
		txn.Set(foo{fooA: fooA{ID: fooID("f1"), A: 7}})
		txn.Set(bar{barA: barA{ID: barID("b11"), A: "one", FooID: fooID("f1")}})
		txn.Set(bar{barA: barA{ID: barID("b12"), A: "two", FooID: fooID("f1")}})
		txn.Set(foo{fooA: fooA{ID: fooID("f2"), A: 42}})
	})

	for snapshot := range tap {
		var f1, f2 foo
		var b11, b12 bar
		if !snapshot.Get(fooID("f1"), &f1) || f1.B < 13 {
			continue
		}
		if !snapshot.Get(fooID("f2"), &f2) || f2.B < 42 {
			continue
		}
		if !snapshot.Get(barID("b11"), &b11) || b11.B == "" {
			continue
		}
		if !snapshot.Get(barID("b12"), &b12) || b12.B == "" {
			continue
		}
		require.Equal(t, 13, f1.B)
		require.Equal(t, 42, f2.B)
		require.Equal(t, "one7", b11.B)
		require.Equal(t, "two7", b12.B)
		break
	}
	require.NoError(t, group.Context().Err()) // not timed out

	a.Do(func(txn Transaction) {
		var f1 foo
		MustGet(txn, fooID("f1"), &f1)
		f1.A = 77
		txn.Set(f1)
	})

	for snapshot := range tap {
		var f1 foo
		MustGet(snapshot, fooID("f1"), &f1)
		var b11, b12 bar
		MustGet(snapshot, barID("b11"), &b11)
		MustGet(snapshot, barID("b12"), &b12)
		if f1.B == 13 || b11.B == "one7" || b12.B == "two7" {
			continue
		}
		require.Equal(t, 83, f1.B)
		require.Equal(t, "one77", b11.B)
		require.Equal(t, "two77", b12.B)
		break
	}
	require.NoError(t, group.Context().Err()) // not timed out

	a.Do(func(txn Transaction) {
		var b11 bar
		MustGet(txn, barID("b11"), &b11)
		b11.FooID = fooID("f2")
		txn.Set(b11)
	})

	for snapshot := range tap {
		var f1, f2 foo
		MustGet(snapshot, fooID("f1"), &f1)
		MustGet(snapshot, fooID("f2"), &f2)
		var b11 bar
		MustGet(snapshot, barID("b11"), &b11)
		if f1.B == 83 || f2.B == 42 || b11.B == "one77" {
			continue
		}
		require.Equal(t, 80, f1.B)
		require.Equal(t, 45, f2.B)
		require.Equal(t, "one42", b11.B)
		break
	}
	require.NoError(t, group.Context().Err()) // not timed out
}
