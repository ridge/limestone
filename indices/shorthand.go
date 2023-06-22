package indices

import (
	"fmt"
	"regexp"
	"strings"
)

var tokenRx = regexp.MustCompile(`^([-+])?([A-Z][A-Za-z0-9_]*)(!)?$`)

// Index is a shorthand for specifying common kinds of indices. An index is
// described by a string containing space-separated words of two kinds:
//
// Fields, in forms "Field" and "Field!".
//
// Filters, in the forms "-Field" and "+Field".
//
// Resulting index will contain all fields in order of their appearance in the
// description. For fields of form "Field!" only entities with non-zero values
// of this field will be included in the index.
//
// A filter of form "-Field" causes all entities where the specified field has a
// nonzero value of its type to be excluded from the index. A filter of the form
// "+Field" has the reverse effect: it excludes those entities where the
// specified field has the zero value of its type. In particular, when the field
// is boolean, "-Field" means "only entities where Field is false", and "+Field"
// means "only entities where Field is true".
//
// Fields and filters can be freely mixed in the description.
//
// Examples:
//
// Index("Test") => FieldIndex("Test")
// Index("Test1 Test2") => CompoundIndex(FieldIndex("Test1"), FieldIndex("Test2"))
// Index("Number! Price -Deleted") => IndexIfZero("Deleted", CompoundIndex(
//
//	FieldIndex("Number", SkipZeros), FieldIndex("Price")))
//
// Index("Value! +Parent") => IndexIfNonzero("Parent",
//
//	FieldIndex("Value", SkipZeros))
func Index(def string) Definition {
	var fieldIndices []Definition
	var filtersZero, filtersNonzero []string

	fields := strings.Fields(def)
	for _, field := range fields {
		m := tokenRx.FindStringSubmatch(field)
		if m == nil {
			panic(fmt.Errorf("%q is not a valid field", field))
		}
		switch {
		case m[1] != "" && m[3] == "!":
			panic(fmt.Errorf("%q: %s and ! modifiers are mutually exclusive", field, m[1]))
		case m[1] == "-":
			filtersZero = append(filtersZero, m[2])
		case m[1] == "+":
			filtersNonzero = append(filtersNonzero, m[2])
		case m[3] == "!":
			fieldIndices = append(fieldIndices, FieldIndex(m[2], SkipZeros))
		default:
			fieldIndices = append(fieldIndices, FieldIndex(m[2]))
		}
	}

	var index Definition

	switch len(fieldIndices) {
	case 0:
		index = IdentityIndex()
	case 1:
		index = fieldIndices[0]
	default:
		index = CompoundIndex(fieldIndices...)
	}

	for _, filter := range filtersZero {
		index = IndexIfZero(filter, index)
	}
	for _, filter := range filtersNonzero {
		index = IndexIfNonzero(filter, index)
	}
	return index
}

// UniqueIndex is the same as Index, but the produced index is unique.
func UniqueIndex(def string) Definition {
	return UniquifyIndex(Index(def))
}
