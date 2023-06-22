package wire

import "encoding/json"

// Manifest describes where the transaction log is stored
type Manifest struct {
	Version int
	Topic   string

	Maintenance bool `json:",omitempty"` // If true, only maintenance tools are allowed to touch the database

	Audit json.RawMessage `json:",omitempty"` // extra metadata; exact format decoupled from Limestone
}
