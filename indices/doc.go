// Package indices contains index definitions for Limestone.
//
// Limestone keeps entities in a memory database indexed for efficient access.
// An index can be based on one or more properties of an entity. To define an
// entity kind with indices, first make some index definitions:
//
//	var (
//	    IndexProjectID = limestone.FieldIndex("ProjectID")
//	    IndexProjectIDName = limestone.UniqueIndex(
//	        limestone.ConditionalIndexWhereFalse("Deleted",
//	            limestone.CompoundIndex(
//	                IndexProjectID,
//	                limestone.FieldIndex("Name", limestone.IgnoreCase),
//	            )
//	        )
//	    )
//	)
//
// IndexProjectID defines a simple index on the ProjectID property.
//
// IndexProjectIDName is a more complex example: it is a unique compound index
// on the (ProjectID, Name) pair, where name is sorted and matched
// case-insensitively, conditional on the Deleted property being false. Entites
// where Deleted is true are excluded from the index, cannot be found in it
// using snapshot.Search or enumerated with snapshot.All, and the uniqueness
// requirement imposed by limestone.UniqueIndex does not apply to them.
//
// See the documentation for the individual *Index* functions in this package
// for what kinds of indices they can define.
//
// When defining a kind, list the desired index definitions after the first
// argument to KindOf:
//
//	var KindCluster = limestone.KindOf(cluster{}, IndexProjectID, IndexProjectIDName)
//
// Index definitions like IndexProjectID are immutable and can be shared among many
// kinds, given that they all contain the properties required by the index. This
// produces independent indices for each kind.
//
// Each kind has an implicit unique index on the identity field. You don't have
// to define this index.
//
// When querying a snapshot for entities, the Search method expects an index
// definition to choose the desired index. This is why they are assigned to
// variables in the snippet above.
//
//	iter := snapshot.Search(KindCluster, IndexProjectIDName, project.ID, name)
//
// Every index expects a certain number of arguments of certain types passed to
// Search. For a compound index like IndexProjectIDName, they are the types of
// the participating properties. The types must match exactly. For example, if
// the ProjectID property is of a string-based project.ID type, the
// corresponding argument to Search must be of that type, and cannot be a plain
// string.
//
// One can pass fewer arguments to Search than the index expects:
//
//	iter := snapshot.Search(KindCluster, IndexProjectIDName, project.ID)
//
// This finds all clusters with the specified project ID where Deleted is false
// (because entities where Deleted is true are excluded from the index as
// specified by ConditionalIndexWhereFalse).
//
// Finally, one can call Search with just two arguments to enumerate all indexed
// entities:
//
//	iter := snapshot.All(KindCluster, IndexProjectIDName)
//
// (This iterator will still skip entities where Deleted is true.)
//
// Search always enumerates entities in the order given by the index. For
// example, the last snippet above will enumerate them in ascending order of
// ProjectID, then among those with the same ProjectID, in ascending order of
// Name.
//
// # Indexable types
//
// All integer types, strings, booleans, and all named types based on them are
// considered indexable. Fields of such types and pointers to those are
// supported by limestone.FieldIndex and expected by Search. These types are
// also the ones supported as return values of custom index functions passed to
// CustomIndex.
//
// Any other type can be made indexable by implementing a method
//
//	IndexKey() ([]byte, bool)
//
// The method should be a pure function that serializes the value as a byte
// sequence which later participates in lexicographical ordering. It may be
// concatenated with other such byte sequences if the index is compound.
//
// The second return value should be true if the value is nonempty (this is
// taken into account by FieldIndex with the SkipZeros option).
package indices
