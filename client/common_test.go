package client

import (
	"encoding/json"

	"github.com/ridge/limestone/wire"
)

var (
	testTxn1 = wire.Transaction{
		Source:  wire.Source{Producer: "foo"},
		Session: 42,
		Changes: wire.Changes{
			"apple": wire.KindChanges{
				"a": wire.Diff{
					"Color": json.RawMessage(`"red"`),
				},
			},
		},
	}
	testTxn2 = wire.Transaction{
		Source:  wire.Source{Producer: "bar"},
		Session: 24,
		Changes: wire.Changes{
			"orange": wire.KindChanges{
				"o": wire.Diff{
					"Color": json.RawMessage(`"orange"`),
				},
			},
		},
	}
)
