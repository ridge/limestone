package compare

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

func TestCompareBehaviorTxn(t *testing.T) {
	t.Parallel()

	tsNext := tsNext(time.Now())
	tapTxn := make(chan Txn, 1)
	tapDif := make(chan Difference, 1)

	txnDefault := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     tsNext(),
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"default-kind:": map[string]wire.Diff{
					"default-id": map[string]json.RawMessage{
						"default-field": json.RawMessage(`"default-value"`),
					},
				},
			},
		},
	}
	txnRemoved := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     tsNext(),
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"before-kind": map[string]wire.Diff{
					"before-id": map[string]json.RawMessage{
						"before-field": json.RawMessage(`"before-value"`),
					},
				},
			},
		},
	}
	txnAdded := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     tsNext(),
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"after-kind": map[string]wire.Diff{
					"after-id": map[string]json.RawMessage{
						"after-field": json.RawMessage(`"after-value"`),
					},
				},
			},
		},
	}
	txnChangedBefore := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     tsNext(),
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"changed-kind": map[string]wire.Diff{
					"changed-id": map[string]json.RawMessage{
						"changed-field": json.RawMessage(`"old-value"`),
					},
				},
			},
		},
	}
	txnChangedAfter := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     txnChangedBefore.TS,
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"changed-kind": map[string]wire.Diff{
					"changed-id": map[string]json.RawMessage{
						"changed-field": json.RawMessage(`"new-value"`),
					},
				},
			},
		},
	}
	txnUnchanged := wire.IncomingTransaction{
		Transaction: wire.Transaction{
			TS:     tsNext(),
			Source: wire.Source{Producer: "default-producer"},
			Changes: map[string]wire.KindChanges{
				"unchanged-kind:": map[string]wire.Diff{
					"unchanged-id": map[string]json.RawMessage{
						"unchanged-field": json.RawMessage(`"unchanged-value"`),
					},
				},
			},
		},
	}

	oldStream := transactions(txnDefault, txnRemoved, txnChangedBefore, txnUnchanged)
	newStream := transactions(txnDefault, txnAdded, txnChangedAfter, txnUnchanged)

	g := test.GroupWithTimeout(t, 1*time.Second)
	g.Spawn("limestone-compare", parallel.Exit, func(ctx context.Context) error {
		return Run(ctx, oldStream, newStream, tapTxn, tapDif)
	})

	assert.Equal(t, Txn{Transaction: txnDefault.Transaction, Type: Unchanged}, <-tapTxn)
	assert.Equal(t, Txn{Transaction: txnRemoved.Transaction, Type: Removed}, <-tapTxn)
	assert.Equal(t, Difference{
		ID:      "before-id",
		Kind:    "before-kind",
		DiffOld: txnRemoved.Transaction.Changes["before-kind"]["before-id"],
	}, <-tapDif)
	assert.Equal(t, Txn{Transaction: txnAdded.Transaction, Type: Added}, <-tapTxn)
	assert.Equal(t, Difference{
		ID:      "after-id",
		Kind:    "after-kind",
		DiffNew: txnAdded.Transaction.Changes["after-kind"]["after-id"],
	}, <-tapDif)
	assert.Equal(t, Txn{Transaction: txnChangedAfter.Transaction, Type: Changed}, <-tapTxn)
	assert.Equal(t, Difference{
		ID:      "changed-id",
		Kind:    "changed-kind",
		DiffNew: txnChangedAfter.Changes["changed-kind"]["changed-id"],
		DiffOld: txnChangedBefore.Changes["changed-kind"]["changed-id"],
	}, <-tapDif)
	assert.Equal(t, Txn{Transaction: txnUnchanged.Transaction, Type: Unchanged}, <-tapTxn)

	select {
	case _, ok := <-tapTxn:
		require.False(t, ok)
	case <-g.Done():
		require.NoError(t, g.Wait())
	}

	select {
	case _, ok := <-tapDif:
		require.False(t, ok)
	case <-g.Done():
		require.NoError(t, g.Wait())
	}
}

// FIXME (tomer): turn it into a benchmark
func TestComparePerformance(t *testing.T) {
	t.Skip() // used when working on the limestone compare

	tsNext := tsNext(time.Now())
	const timeout = 20 * time.Second
	const count = 1000000
	tapTxn := make(chan Txn, 1000)
	tapDif := make(chan Difference)

	var txns []wire.IncomingTransaction
	for i := 0; i < count; i++ {
		txns = append(txns, wire.IncomingTransaction{
			Transaction: wire.Transaction{
				TS:     tsNext(),
				Source: wire.Source{Producer: "default-producer"},
				Changes: map[string]wire.KindChanges{
					"default-kind:": map[string]wire.Diff{
						"default-id": map[string]json.RawMessage{
							"default-field": json.RawMessage(`"default-value"`),
						},
					},
				},
			},
		})
	}

	g := test.GroupWithTimeout(t, timeout)
	newStream := transactions(txns...)
	oldStream := transactions(txns...)
	g.Spawn("limestone-compare", parallel.Exit, func(ctx context.Context) error {
		// FIXME (tomer): accept an entity channel
		return Run(ctx, newStream, oldStream, tapTxn, tapDif)
	})

	deadline := time.After(timeout)
	for i := 0; i < count; i++ {
		select {
		case <-deadline:
			require.FailNow(t, "reached timeout")
		case v := <-tapTxn:
			require.Equal(t, Unchanged, v.Type)
		}
	}
	if _, open := <-tapDif; open {
		require.FailNow(t, "entities tap is not closed")
	}
	if _, open := <-tapTxn; open {
		require.FailNow(t, "transactions tap is not closed")
	}
}

func transactions(txns ...wire.IncomingTransaction) <-chan *wire.IncomingTransaction {
	ch := make(chan *wire.IncomingTransaction, len(txns)+1)
	for i := range txns {
		ch <- &txns[i]
	}
	ch <- nil
	return ch
}

func tsNext(t time.Time) func() *time.Time {
	return func() *time.Time {
		next := t
		t = t.Add(600)
		return &next
	}
}

func TestUnequalEntities(t *testing.T) {
	changesOld := wire.Changes{
		"kind1": map[string]wire.Diff{
			"id1": map[string]json.RawMessage{
				"property": must.OK1(json.Marshal("value")),
			},
		},
		"kindboth": map[string]wire.Diff{
			"idboth1": map[string]json.RawMessage{
				"propertyBoth": must.OK1(json.Marshal("value")),
			},
			"idboth2": map[string]json.RawMessage{
				"propertyBoth": must.OK1(json.Marshal("value1")),
			},
			"idboth3": map[string]json.RawMessage{
				"property1": must.OK1(json.Marshal("value")),
			},
		},
		"sameKind": map[string]wire.Diff{
			"sameID": map[string]json.RawMessage{
				"sameProperty": must.OK1(json.Marshal("same")),
			},
		},
	}
	changesNew := wire.Changes{
		"kind2": map[string]wire.Diff{
			"id2": map[string]json.RawMessage{
				"property": must.OK1(json.Marshal("value")),
			},
		},
		"kindboth": map[string]wire.Diff{
			"idboth1": map[string]json.RawMessage{
				"propertyBoth": must.OK1(json.Marshal("value")),
			},
			"idboth2": map[string]json.RawMessage{
				"propertyBoth": must.OK1(json.Marshal("value2")),
			},
			"idboth3": map[string]json.RawMessage{
				"property2": must.OK1(json.Marshal("value")),
			},
		},
		"sameKind": map[string]wire.Diff{
			"sameID": map[string]json.RawMessage{
				"sameProperty": must.OK1(json.Marshal("same")),
			},
		},
	}
	diffs := unequalEntities(changesOld, changesNew)

	assert.Len(t, diffs, 4)
	assert.Contains(t, diffs, Difference{
		ID:      "id1",
		Kind:    "kind1",
		DiffOld: changesOld["kind1"]["id1"],
	})
	assert.Contains(t, diffs, Difference{
		ID:      "id2",
		Kind:    "kind2",
		DiffNew: changesNew["kind2"]["id2"],
	})
	assert.Contains(t, diffs, Difference{
		ID:      "idboth2",
		Kind:    "kindboth",
		DiffNew: changesNew["kindboth"]["idboth2"],
		DiffOld: changesOld["kindboth"]["idboth2"],
	})
	assert.Contains(t, diffs, Difference{
		ID:      "idboth3",
		Kind:    "kindboth",
		DiffNew: changesNew["kindboth"]["idboth3"],
		DiffOld: changesOld["kindboth"]["idboth3"],
	})
}
