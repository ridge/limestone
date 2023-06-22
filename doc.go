// Package limestone is a framework for transactional access to entities
// that are described by incremental updates persistently stored
// as Kafka messages.
//
// The name comes from the Tectonic theme: limestone is a sedimentary rock,
// and the local database functions by accretion of new events over time.
//
// # Entity kinds
//
// Limestone manages entities of several "kinds" (types). Each kind
// is associated with one Go structure type and one Kafka topic for updates.
// Such a structure consists of one or more sections written by different
// services (producers). Sections are nested structures embedded in the entity
// structure. Each section can only have one producer that is allowed to write
// the fields in it.
//
// It is not necessary for every service to use a top-level entity structure
// that embeds all the sections. A service only has to use the sections
// that it is interested in reading or writing. Usually, a shared package
// declares all the section structures, and the service declares its own
// entity structure where it lists only the sections it needs. It is possible
// for a service to add extra sections, as well as extra fields, to store its
// own private data. Note that all fields of one entity kind must be uniquely
// named across sections.
//
// # Access to entities
//
// Read-only access is done through a snapshot. A snapshot can be obtained
// using the Snapshot method of the database. Acquiring a snapshot is a very
// cheap operation which does not need a global lock. The snapshot captures
// a static state of the world at a particular moment, which stays the same
// even if more changes to the entities keep happening.
//
// It is not allowed to modify slices, maps and structures available by
// references from an entity. To make changes to entities, one needs
// a transaction.
//
// To initiate a transaction, call the database's Do or DoE method. This is
// relatively expensive because it acquires a global lock. For this reason,
// keep the amount of code inside a transaction to a minimum.
//
// To make changes, you need to obtain entities from the transaction, modify
// them, and insert them into the transaction. It is still not allowed
// to modify slices, maps and structures referred to by the original entity.
// To modify e.g. a slice, replace the value of a slice-typed field with
// a new, modified slice. New entities can be created by inserting new
// structures with new unique identity values.
//
// Snapshots are safe to use concurrently but transactions are not. A single
// iterator obtained from a Search call on a transaction or snapshot must not be
// called concurrently, although separate calls to Search return independent
// iterators that can be called in parallel with each other.
//
// # Wake-up
//
// When changes happen outside the current service, Limestone applies them
// to the local state as transactions. When this happens, the service's
// optional WakeUp handler is invoked with the unfinished transaction and
// the list of all entities touched in this update. The handler then has
// a chance to react to these changes, and make more changes. The incoming
// changes along with the extra changes made by the handler, will be
// committed as soon as the handler returns. A nil handler is equivalent
// to a handler that does nothing.
//
// Limestone will always group as many queued incoming updates as possible
// into one transaction.
//
// # Limestone tags
//
// Members of section structures can be labeled with optional limestone tags.
// The value of a limestone tag is a comma-separated list of options:
//
// * const: the field cannot be modified once the entity is created. If the
// field is a slice, map or pointer, the contents referred by it cannot be
// changed. Limestone will panic if the local code or an incoming update
// tries to modify a const field. This tag can also be used on an anonymous
// field (embedding structure), which makes it apply to all fields in that
// structure, recursively.
//
// * required: the field is not allowed to have an empty value.
// Note that an empty but non-nil slice or map is not an empty value.
//
// * identity: the field is the primary key of the entity. There must be
// exactly one such field per entity kind. This option implies const and
// required. The best practice is to name this field ID. The field must be
// a string or a type whose underlying data type is string. It is recommended
// that each package for a specific entity exports a string-based type ID
// and declares the identity field to be this type. Other entities referring
// to this entity by ID should declare their foreign-key properties
// with this type (see ID and BarIDs in the example below).
//
// * name=NAME: sets the wire (JSON) name of the field. By default, the wire
// name is the same as the Go field name. This option should be used to avoid
// conflicts when two fields in different sections have the same Go name.
//
// * - (hyphen): declares the field as hidden. Such a field is not exported to
// Kafka and not updated from incoming Kafka messages. It is used to store
// non-persistent data local to the service. Such fields still can only be
// modified the usual way, through transactions. However, if the field is a
// pointer, a slice or a map, the target contents can be modified freely even
// between transactions. It is possible to declare hidden fields not only in
// section structures, but also in the top-level entity structure. This tag can
// also be applied to an anonymous field (embedded structure).
//
// There are also structure-level tags. They can be declared in the entity
// structure or any of the per-section sub-structures. Because Go does not
// allow structure-level tags, they should be attached to a Meta field.
// Add an anonymous limestone.Meta field to your structure and attach
// the structure-level tags to it. The limestone.Meta type itself has zero
// size.
//
// * name=NAME: sets the Kafka topic name for the entity kind. Each section
// structure must declare the same name.
//
// * producer=PRODUCER: declares which service can write the fields in this
// section. The specified pattern is matched against the producer name
// specified as Source.Producer when initializing Limestone. The pattern can
// contain | to delimit alternatives: producer=allocator|compute-api matches
// producer names "allocator" an "compute-api". The setting applies to all
// fields in the section. Different sections within the same entity can
// declare different producer names. Fields contained in structures with no
// producer name specified, can only be modified by administrative Kafka
// messages without a producer value (all outgoing messages produced by
// Limestone have a producer value).
//
// # Example
//
// The following example demonstrates the use of some of the features
// described above.
//
//	// Package foo declares the sections of the Foo entity
//	package foo
//
//	import (
//	    "go.tectonic.network/kraken/entities/bar"
//	    "go.tectonic.network/kraken/limestone"
//	)
//
//	// ID is a unique foo ID
//	type ID string
//
//	// Creator is the section of Foo written by foo-creator
//	type Creator struct {
//	    limestone.Meta `limestone:"name=foo,producer=foo-creator"`
//
//	    ID     ID       `limestone:"identity"`
//	    Size   int      `limestone:"required"`
//	    BarIDs []bar.ID `limestone:"const,name=bar_ids"`
//	}
//
//	// Handler is the section of Foo written by foo-handler
//	type Handler struct {
//	    limestone.Meta `limestone:"name=foo,producer=foo-handler"`
//
//	    Status string
//	}
//
//	// ...in a service such as foo-handler:
//	type Foo struct {
//	    foo.Creator
//	    foo.Handler
//
//	    lastChecked time.Time `limestone:"-"`
//	}
//
// # Deadline
//
// The entity structure can define an optional Deadline method:
//
//	func (Foo) Deadline() time.Time
//
// If this method exists, Limestone will call it after each transaction where
// the entity is modified. If it returns a nonzero Time value, Limestone will
// arrange a wake-up call soon after the specified time, and the entity will
// be included in the list passed to the wake-up handler even if it hasn't
// been modified. It can be combined with other reasons to call the handler.
//
// limestone.NoDeadline constant is a handy way to explicitly return zero Time
// value from the handler.
//
// If the entity is modified before the time returned by Deadlne,
// the existing deadline is ignored, and Deadline is called again,
// so it has the opportunity to specify a new deadline if needed.
//
// The Deadline method must be a pure function that depends on nothing but its
// receiver.
//
// Typical use cases for Deadline are timeouts (for example, detecting that
// the instance has been in the starting state for too long) and periodic checks.
// For the latter case, it's typical to use a hidden field, like lastChecked
// in the example above, to record the time of the last check.
//
// # Survive
//
// The entity structure can define an optional Survive method:
//
//	func (Foo) Survive() bool
//
// If this method exists, Limestone will call it to decide whether the entity is
// worth keeping in memory. If Survive returns false, Limestone will forget
// about the entity, and any future incoming updates regarding it will be
// ignored. The exact rules are as follows:
//
//   - If an incoming transaction changes an entity previously seen by WakeUp so
//     that Survive returns false, the entity will be featured in one last WakeUp
//     call so that the service can take cleanup actions such as deleting a
//     real-world object.
//   - If an incoming transaction introduces an entity for which Survive returns
//     false right away, or if it modifies an existing entity not previously seen
//     by WakeUp so that Survive returns false, the entity won't be featured in a
//     WakeUp call.
//   - If an entity is changed in a local transaction (WakeUp or Do) so that
//     Survive returns false, the change will be submitted to the database, and
//     the entity won't be featured in any future WakeUp calls.
//
// The Survive method must be a pure function that depends on nothing but its
// receiver.
//
// The typical use case for Survive is to discard entities with Deleted flag.
//
// # Initial catch-up
//
// When the service starts, Limestone begins to catch up with Kafka history.
// While it is happening Snapshot() method returns nil, DoE() and Do() panic.
// WaitReady() can be used to block until the initial catch-up is finished.
//
// During initial catch-up Limestone reads the entire history of relevant Kafka
// updates to build the current state, and puts all these changes into a single
// transaction. After that, the wake-up handler is called once with the list of
// all entities that exist.
//
// Deadline handlers will be called as usual after the initial transaction is
// complete.
//
// # Command line
//
// Importing the limestone package adds the following option to global set
// of command-line flags:
//
//	--limestone-server string             domain name and port to access the Limestone server (default "localhost:10007")
//
// When DefaultClient() is used as Config.Client (which is normal outside
// tests), the Limestone server specified on the command line, or the default
// value, is used.
//
// # Component testing
//
// Limestone can be used for component testing with the mock Kafka implmentation
// (kraken/kafka/mock).
//
// As an example, a test scenario for the allocator might look like this:
//
// 1. Initialize mock Kafka; insert some initial messages into it if necessary.
//
//	mockKafka := mock.New()
//	require.NoError(t, limestone.WriteAdminMessages(ctx, mockKafka, Partner{...}, DataCenter{...})
//
// 2. Initialize the allocator connected to mock Kafka.
//
//	go allocatorServer.Run(ctx, k)
//
// 3. Initialize a test agent imitating the manager connected to mock Kafka.
//
//	mockManager := limestone.New(limestone.Config{
//	    Kafka:    k,
//	    Entities: limestone.KindList{kindFleet},
//	    Source:   limestone.Source{Producer: "allocator"},
//	    Logger:   tlog.Get(ctx),
//	})
//	go mockManager.Run(ctx)
//
// 4. Initialize a test agent imitating the DCM connected to mock Kafka.
//
//	tapDCM := make(chan Snapshot)
//	mockDCM := limestone.New(limestone.Config{
//	    Kafka:    k,
//	    Entities: limestone.KindList{kindInstance},
//	    Source:   limestone.Source{Producer: "example-service"},
//	    Logger:   tlog.Get(ctx),
//	    DebugTap: tapDCM,
//	})
//	go mockDCM.Run(ctx)
//
// 5. As mock manager, create a fleet.
//
//	require.NoError(t, mockManager.WaitReady(ctx))
//	mockManager.Do(func(txn limestone.Transaction) {
//	    txn.Set(Fleet{...})
//	})
//
// 6. As mock DCM, wait for the expected number of instances to be created by the allocator.
//
//	for snapshot := range tapDCM {
//	    iter := snapshot.Search(kindInstance, indexFleetID, fleetID)
//	    n := 0
//	    var inst Instance
//	    for iter(&inst) {
//	        n++
//	    }
//	    if n == expectedNumber {
//	        break
//	    }
//	}
package limestone
