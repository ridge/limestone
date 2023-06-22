package wire

// Filter describes a filter for relevant kinds and property names.
//
// The map has an entry for each kind, with the kind name as key and the list of
// relevant property names as value.
//
// A nil value of a map entry means all properties of the kind are relevant.
//
// A nil value of the map itself means all properties of all kinds are relevant.
type Filter map[string][]string

// A Request is sent from client to server as the mandatory first WS message
type Request struct {
	Version int
	Last    Position
	Filter  Filter
	Compact bool // OK to collapse series of transactions and strip events
}

// Notification is a packet sent from server to client
//
// If more than one of the properties are present, the packet is equivalent to
// several packets containing one each, in the order in which they are declared
// below.
type Notification struct {
	// Reports a fatal error. The server disconnects immediately after.
	Err ErrMismatch `json:",omitempty"`

	// An incoming transaction
	Txn *IncomingTransaction `json:",omitempty"`

	// The hot end of the stream has been reached
	Hot bool `json:",omitempty"`
}
