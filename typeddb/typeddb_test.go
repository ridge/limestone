package typeddb

import (
	"testing"

	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/meta"
	"github.com/stretchr/testify/require"
)

type fooID string
type foo struct {
	meta.Meta `limestone:"name=foo,producer=p"`
	ID        fooID `limestone:"identity"`
}
type barID string
type bar struct {
	meta.Meta `limestone:"name=bar,producer=p"`
	ID        barID `limestone:"identity"`
	FooID     fooID
}

var (
	kindFoo       = KindOf(foo{})
	barIndexFooID = indices.FieldIndex("FooID")
	kindBar       = KindOf(bar{}, barIndexFooID)
)

func testEnv() *TypedDB {
	db := New([]*Kind{kindFoo, kindBar})
	txn, ctrl := db.Transaction()
	SetMany(txn,
		[]foo{
			{ID: "foo1"},
			{ID: "foo2"},
		},
		[]bar{
			{ID: "bar1", FooID: "foo1"},
			{ID: "bar2", FooID: "foo2"},
		},
	)
	ctrl.Commit()
	return db
}

func checkSnapshot(t *testing.T, s Snapshot) {
	var f foo
	require.True(t, s.Get("foo1", &f))
	require.Equal(t, foo{ID: "foo1"}, f)

	require.False(t, s.Get("foo0", &f))

	iter := s.All(kindFoo)
	require.True(t, iter(&f))
	require.Equal(t, foo{ID: "foo1"}, f)
	require.True(t, iter(&f))
	require.Equal(t, foo{ID: "foo2"}, f)
	require.False(t, iter(&f))

	var b bar
	iter = s.Search(kindBar, barIndexFooID, fooID("foo1"))
	require.True(t, iter(&b))
	require.Equal(t, bar{ID: "bar1", FooID: "foo1"}, b)
	require.False(t, iter(&b))

	iter = s.Search(kindBar, barIndexFooID, fooID("foo0"))
	require.False(t, iter(&b))
}

func TestSnapshot(t *testing.T) {
	db := testEnv()
	checkSnapshot(t, db.Snapshot())
}

func TestTransaction(t *testing.T) {
	db := testEnv()
	txn, ctrl := db.Transaction()
	checkSnapshot(t, txn)
	checkSnapshot(t, txn.Before())

	ctrl.SetByEID(EID{Kind: kindFoo, ID: "foo3"}, foo{ID: "foo3"})
	txn.Set(bar{ID: "bar2", FooID: "foo3"})
	ctrl.Prune(EID{Kind: kindFoo, ID: "foo2"})

	checkSnapshot(t, txn.Before())
	checkSnapshot(t, db.Snapshot())

	require.Nil(t, ctrl.GetByEID(EID{Kind: kindFoo, ID: "foo2"}))
	require.Equal(t, foo{ID: "foo3"}, ctrl.GetByEID(EID{Kind: kindFoo, ID: "foo3"}))

	require.Equal(t, map[EID]Change{
		{Kind: kindFoo, ID: "foo3"}: {After: foo{ID: "foo3"}},
		{Kind: kindBar, ID: "bar2"}: {Before: bar{ID: "bar2", FooID: "foo2"}, After: bar{ID: "bar2", FooID: "foo3"}},
	}, ctrl.Changes())

	ctrl.Commit()

	s := db.Snapshot()

	var f foo
	require.False(t, s.Get("foo2", &f))
	require.True(t, s.Get("foo3", &f))
	require.Equal(t, foo{ID: "foo3"}, f)

	var b bar
	require.True(t, s.Get("bar2", &b))
	require.Equal(t, bar{ID: "bar2", FooID: "foo3"}, b)
	iter := s.Search(kindBar, barIndexFooID, fooID("foo2"))
	require.False(t, iter(&b))
	iter = s.Search(kindBar, barIndexFooID, fooID("foo3"))
	require.True(t, iter(&b))
	require.Equal(t, bar{ID: "bar2", FooID: "foo3"}, b)
}

func TestTransactionCanceled(t *testing.T) {
	db := testEnv()
	txn, ctrl := db.Transaction()
	checkSnapshot(t, txn)
	checkSnapshot(t, txn.Before())

	ctrl.SetByEID(EID{Kind: kindFoo, ID: "foo3"}, foo{ID: "foo3"})
	txn.Set(bar{ID: "foo2", FooID: "foo3"})
	ctrl.Prune(EID{Kind: kindFoo, ID: "foo2"})

	checkSnapshot(t, txn.Before())
	checkSnapshot(t, db.Snapshot())

	ctrl.Cancel()

	checkSnapshot(t, db.Snapshot())
}

func TestAnnotate(t *testing.T) {
	db := testEnv()

	txn, ctrl := db.Transaction()
	defer ctrl.Cancel()

	txn.Annotate("foo", "dog")
	txn.Annotate("bar", "cat")
	txn.Annotate("foo", "dog")

	require.Equal(t, map[string]string{"foo": "dog", "bar": "cat"}, ctrl.Annotations())
	require.Panics(t, func() { txn.Annotate("foo", "rabbit") })
}

func TestMustGet(t *testing.T) {
	db := testEnv()
	s := db.Snapshot()

	var f foo
	require.NotPanics(t, func() { MustGet(s, "foo1", &f) })
	require.Equal(t, foo{ID: "foo1"}, f)
	require.Panics(t, func() { MustGet(s, "foo0", &f) })
}
