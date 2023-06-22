package server

import (
	"encoding/json"
	"testing"

	"github.com/ridge/limestone/wire"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	changes := wire.Changes{
		"apple": wire.KindChanges{
			"a": wire.Diff{
				"Color": json.RawMessage(`"red"`),
				"Size":  json.RawMessage(`1`),
			},
			"b": wire.Diff{
				"Size": json.RawMessage(`2`),
			},
		},
		"orange": wire.KindChanges{
			"a": wire.Diff{
				"Color": json.RawMessage(`"orange"`),
				"Mass":  json.RawMessage(`3`),
			},
			"b": wire.Diff{
				"Mass": json.RawMessage(`4`),
			},
		},
	}

	require.Equal(t, changes, compileFilter(nil)(changes))
	require.Equal(t, wire.Changes{"apple": changes["apple"]},
		compileFilter(wire.Filter{"apple": nil, "kiwi": nil})(changes))
	require.Nil(t, compileFilter(wire.Filter{"kiwi": nil})(changes))
	require.Equal(t, wire.Changes{
		"orange": wire.KindChanges{
			"a": wire.Diff{
				"Color": json.RawMessage(`"orange"`),
			},
		},
	}, compileFilter(wire.Filter{
		"apple":  []string{"Shape"},
		"orange": []string{"Shape", "Color"},
	})(changes))
}
