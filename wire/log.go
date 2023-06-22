package wire

import (
	"sort"

	"github.com/ridge/must/v2"
	"go.uber.org/zap/zapcore"
)

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Source with zap.Object
func (s Source) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if s.Producer != "" {
		e.AddString("producer", string(s.Producer))
	}
	if s.Instance != "" {
		e.AddString("instance", s.Instance)
	}
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Transaction with zap.Object
func (txn Transaction) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if txn.TS != nil {
		e.AddTime("ts", *txn.TS)
	}
	must.OK(e.AddObject("source", txn.Source))
	e.AddInt64("session", txn.Session)
	must.OK(e.AddObject("changes", txn.Changes))
	if txn.Audit != nil {
		e.AddString("audit", string(txn.Audit))
	}
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of IncomingTransaction with zap.Object
func (txn IncomingTransaction) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("position", string(txn.Position))
	return txn.Transaction.MarshalLogObject(e)
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Changes with zap.Object
func (c Changes) MarshalLogObject(e zapcore.ObjectEncoder) error {
	for kind, byID := range c {
		must.OK(e.AddArray(kind, byID))
	}
	return nil
}

type kindChangesForLog struct {
	id   string
	diff Diff
}

func (kcfl kindChangesForLog) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("id", kcfl.id)
	return e.AddObject("diff", kcfl.diff)
}

// MarshalLogArray implements zapcore.ArrayMarshaler to allow logging of KindChanges with zap.Object
func (kc KindChanges) MarshalLogArray(e zapcore.ArrayEncoder) error {
	kcfls := make([]kindChangesForLog, 0, len(kc))
	for id, diff := range kc {
		kcfls = append(kcfls, kindChangesForLog{id, diff})
	}
	sort.Slice(kcfls, func(i, j int) bool {
		return kcfls[i].id < kcfls[j].id
	})
	for _, kcfl := range kcfls {
		must.OK(e.AppendObject(kcfl))
	}
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Diff with zap.Object
func (d Diff) MarshalLogObject(e zapcore.ObjectEncoder) error {
	for prop, value := range d {
		e.AddString(prop, string(value))
	}
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Notification with zap.Object
func (n Notification) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if n.Err != "" {
		e.AddString("Err", n.Err.Error())
	}
	if n.Txn != nil {
		must.OK(e.AddObject("txn", n.Txn))
	}
	if n.Hot {
		e.AddBool("hot", true)
	}
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler to allow logging of Manifest with zap.Object
func (m Manifest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt("version", m.Version)
	e.AddString("topic", m.Topic)
	e.AddBool("maintenance", m.Maintenance)
	return nil
}
