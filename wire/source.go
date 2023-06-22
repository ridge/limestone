package wire

import (
	"github.com/ridge/limestone/meta"
)

// Source describes the submitter of a database update
type Source struct {
	Producer meta.Producer `json:",omitempty"` // Generic service name
	Instance string        `json:",omitempty"` // Optional string that makes the pair unique (e.g. shard ID)
}
