package convertdb

import (
	"reflect"

	"github.com/ridge/limestone/indices"
	"github.com/ridge/limestone/meta"
	"github.com/ridge/limestone/typeddb"
)

// Kind describes a particular type of objects handled by the framework.
//
// Similar in purpose to limestone.Kind.
type Kind struct {
	*typeddb.Kind
	before, after meta.Struct
}

// KindList is a list of kinds
type KindList []*Kind

// KindOf creates a Kind for a given object example and index definitions.
//
// Similar in purpose to limestone.KindOf.
func KindOf(example any, indexDefs ...indices.Definition) *Kind {
	t := reflect.TypeOf(example)
	return &Kind{
		Kind:   typeddb.KindOf(example, indexDefs...),
		before: meta.SurveyOld(t),
		after:  meta.SurveyNew(t),
	}
}
