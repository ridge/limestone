package convertdb

import (
	"encoding/json"
	"testing"

	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/stretchr/testify/require"
	"time"
)

var timestamp = time.Date(2020, 2, 20, 20, 20, 20, 0, time.UTC)

func TestFieldAdded(t *testing.T) {
	type fooID string
	type fooCommon struct {
		Meta `limestone:"name=foo,upgrade=common"`
		ID   fooID `limestone:"identity"`
	}
	type fooNew struct {
		Meta  `limestone:"name=foo,upgrade=new"`
		Added int `limestone:"required"`
	}
	type foo struct {
		fooCommon
		fooNew
	}
	kindFoo := KindOf(foo{})
	db := New(KindList{kindFoo})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID": json.RawMessage(must.OK1(json.Marshal("f1"))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1"}}, f)
	f.Added = 1
	txn.Set(f)

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":    json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Added": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())

	txn = db.Transaction(wire.Transaction{TS: &timestamp})
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1"}, fooNew: fooNew{Added: 1}}, f)
	txn.Set(f)
	require.Equal(t, wire.Transaction{TS: &timestamp}, txn.Commit())
}

func TestFieldRemoved(t *testing.T) {
	type fooID string
	type fooCommon struct {
		Meta `limestone:"name=foo,upgrade=common"`
		ID   fooID `limestone:"identity"`
	}
	type fooOld struct {
		Meta    `limestone:"name=foo,upgrade=old"`
		Removed int `limestone:"required"`
	}
	type foo struct {
		fooCommon
		fooOld
	}
	kindFoo := KindOf(foo{})
	db := New(KindList{kindFoo})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":      json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Removed": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1"}, fooOld: fooOld{Removed: 1}}, f)

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID": json.RawMessage(must.OK1(json.Marshal("f1"))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())

	txn = db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"Removed": json.RawMessage(must.OK1(json.Marshal(2))),
				},
			},
		},
		TS: &timestamp,
	})

	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1"}, fooOld: fooOld{Removed: 2}}, f)
	require.Equal(t, wire.Transaction{TS: &timestamp}, txn.Commit())
}

func TestFieldRenamed(t *testing.T) {
	type fooID string
	type fooCommon struct {
		Meta `limestone:"name=foo,upgrade=common"`
		ID   fooID `limestone:"identity"`
	}
	type fooOld struct {
		Meta   `limestone:"name=foo,upgrade=old"`
		Before int `limestone:"required"`
	}
	type fooNew struct {
		Meta  `limestone:"name=foo,upgrade=new"`
		After int `limestone:"required"`
	}
	type foo struct {
		fooCommon
		fooOld
		fooNew
	}
	kindFoo := KindOf(foo{})
	db := New(KindList{kindFoo})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":     json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Before": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1"}, fooOld: fooOld{Before: 1}}, f)
	f.After = f.Before
	txn.Set(f)

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":    json.RawMessage(must.OK1(json.Marshal("f1"))),
					"After": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())
}

func TestOther(t *testing.T) {
	type fooID string
	type fooCommon struct {
		Meta    `limestone:"name=foo,upgrade=common"`
		ID      fooID `limestone:"identity"`
		Tracked int   `limestone:"required"`
	}
	type foo struct {
		fooCommon
	}
	kindFoo := KindOf(foo{})
	db := New(KindList{kindFoo})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":        json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Tracked":   json.RawMessage(must.OK1(json.Marshal(1))),
					"Untracked": json.RawMessage(must.OK1(json.Marshal(2))),
				},
			},
			"bar": wire.KindChanges{
				"b1": wire.Diff{
					"ID":        json.RawMessage(must.OK1(json.Marshal("b1"))),
					"Untracked": json.RawMessage(must.OK1(json.Marshal(3))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1", Tracked: 1}}, f)

	require.Equal(t, wire.Changes{
		"foo": wire.KindChanges{
			"f1": wire.Diff{
				"Untracked": json.RawMessage(must.OK1(json.Marshal(2))),
			},
		},
		"bar": wire.KindChanges{
			"b1": wire.Diff{
				"ID":        json.RawMessage(must.OK1(json.Marshal("b1"))),
				"Untracked": json.RawMessage(must.OK1(json.Marshal(3))),
			},
		},
	}, txn.Other)
	txn.Other["foo"]["f1"]["Untracked"] = json.RawMessage(must.OK1(json.Marshal(22)))
	txn.Other["foo"]["f1"]["Untracked2"] = json.RawMessage(must.OK1(json.Marshal(33)))

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":         json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Tracked":    json.RawMessage(must.OK1(json.Marshal(1))),
					"Untracked":  json.RawMessage(must.OK1(json.Marshal(22))),
					"Untracked2": json.RawMessage(must.OK1(json.Marshal(33))),
				},
			},
			"bar": wire.KindChanges{
				"b1": wire.Diff{
					"ID":        json.RawMessage(must.OK1(json.Marshal("b1"))),
					"Untracked": json.RawMessage(must.OK1(json.Marshal(3))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())
}

func TestForget(t *testing.T) {
	type fooID string
	type fooCommon struct {
		Meta    `limestone:"name=foo,upgrade=common"`
		ID      fooID `limestone:"identity"`
		Tracked int
	}
	type foo struct {
		fooCommon
	}
	kindFoo := KindOf(foo{})
	db := New(KindList{kindFoo})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":      json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Tracked": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooCommon: fooCommon{ID: "f1", Tracked: 1}}, f)
	txn.Forget(f)

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":      json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Tracked": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())

	txn = db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"Untracked": json.RawMessage(must.OK1(json.Marshal(7))),
				},
			},
		},
		TS: &timestamp,
	})
	require.False(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"Untracked": json.RawMessage(must.OK1(json.Marshal(7))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())

	txn = db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"Tracked": json.RawMessage(must.OK1(json.Marshal(2))),
				},
			},
		},
		TS: &timestamp,
	})
	require.False(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, wire.Transaction{TS: &timestamp}, txn.Commit())
}

func TestKindReplaced(t *testing.T) {
	type fooID string
	type fooOld struct {
		Meta    `limestone:"name=foo,upgrade=old"`
		ID      fooID `limestone:"identity"`
		Tracked int
	}
	type foo struct {
		fooOld
	}
	type barID string
	type barNew struct {
		Meta    `limestone:"name=bar,upgrade=new"`
		ID      barID `limestone:"identity"`
		Tracked int
	}
	type bar struct {
		barNew
	}
	kindFoo := KindOf(foo{})
	kindBar := KindOf(bar{})
	db := New(KindList{kindFoo, kindBar})

	txn := db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":      json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Tracked": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	})

	var f foo
	require.True(t, txn.Get(fooID("f1"), &f))
	require.Equal(t, foo{fooOld: fooOld{ID: "f1", Tracked: 1}}, f)
	var b bar
	b.ID = "b1"
	b.Tracked = f.Tracked
	txn.Set(b)

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"bar": wire.KindChanges{
				"b1": wire.Diff{
					"ID":      json.RawMessage(must.OK1(json.Marshal("b1"))),
					"Tracked": json.RawMessage(must.OK1(json.Marshal(1))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())

	txn = db.Transaction(wire.Transaction{
		Changes: wire.Changes{
			"foo": wire.KindChanges{
				"f1": wire.Diff{
					"ID":        json.RawMessage(must.OK1(json.Marshal("f1"))),
					"Untracked": json.RawMessage(must.OK1(json.Marshal(2))),
				},
			},
		},
		TS: &timestamp,
	})

	require.Equal(t, wire.Changes{
		"foo": wire.KindChanges{
			"f1": wire.Diff{
				"Untracked": json.RawMessage(must.OK1(json.Marshal(2))),
			},
		},
	}, txn.Other)
	txn.Other["bar"] = wire.KindChanges{"b1": txn.Other["foo"]["f1"]}
	delete(txn.Other, "foo")

	require.Equal(t, wire.Transaction{
		Changes: wire.Changes{
			"bar": wire.KindChanges{
				"b1": wire.Diff{
					"Untracked": json.RawMessage(must.OK1(json.Marshal(2))),
				},
			},
		},
		TS: &timestamp,
	}, txn.Commit())
}
