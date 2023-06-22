package wire

import "time"

// ActiveSetHeader is the header of the hot start file
type ActiveSetHeader struct {
	Version  int
	Position Position
}

// ActiveObject is a record in the hot start file representing an object in the
// active set
type ActiveObject struct {
	Kind  string
	TS    time.Time // last updated
	Props Diff
}
