package wire

import (
	"encoding/json"

	"time"
)

// Changes is a set of diffs organized by kind and ID
type Changes map[string]KindChanges // kind -> ID -> diff

// Clone returns a deep copy of the Changes
func (c Changes) Clone() Changes {
	if c == nil {
		return nil
	}
	res := Changes{}
	for kind, kc := range c {
		res[kind] = kc.Clone()
	}
	return res
}

// Optimize removes empty diffs and maps from the changes
func (c Changes) Optimize() {
	for kind, kc := range c {
		kc.Optimize()
		if len(kc) == 0 {
			delete(c, kind)
		}
	}
}

// KindChanges is a set of diffs organized by ID, for a single kind
type KindChanges map[string]Diff // ID -> diff

// Clone returns a deep copy of the KindChanges
func (kc KindChanges) Clone() KindChanges {
	if kc == nil {
		return nil
	}
	res := KindChanges{}
	for id, diff := range kc {
		res[id] = diff.Clone()
	}
	return res
}

// Optimize removes empty diffs from the KindChanges
func (kc KindChanges) Optimize() {
	for id, diff := range kc {
		if len(diff) == 0 {
			delete(kc, id)
		}
	}
}

// Transaction is a storage and wire representation of a Limestone transaction
type Transaction struct {
	// Timestamp override
	//
	// When submitting new transactions, leave empty.
	// When reading, assume out-of-band message timestamp if empty.
	// When converting history, set to original transaction timestamp.
	//
	// If set, must precede out-of-band message timestamp.
	// Must be monotonic.
	TS *time.Time `json:",omitempty"`

	Source  Source `json:",omitempty"`
	Session int64  `json:",omitempty"` // for echo cancellation

	Changes Changes

	Audit json.RawMessage `json:",omitempty"` // remote IP, session ID etc; exact format decoupled from Limestone
}

// A Position is an opaque token that can be used to resume reading from a
// known location
type Position string

// Beginning is the Position value that means to read from the beginning of the
// history
const Beginning Position = ""

// IncomingTransaction is a transaction annotated by its position.
// Wire format only.
type IncomingTransaction struct {
	Transaction
	Position Position
}
