package typeddb

import "fmt"

// EID is an entity ID
type EID struct {
	Kind *Kind
	ID   string
}

func (eid EID) String() string {
	if eid.Kind == nil {
		panic(fmt.Sprintf("EID with an empty kind, id=%s", eid.ID))
	}
	return fmt.Sprintf("%s: %s", eid.Kind, eid.ID)
}
