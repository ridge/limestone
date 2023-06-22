// Package kafka contains Kafka clients.
//
// The actual client code is in the subpackages. The package itself is a façade
// that reexports certain symbols from its subpackages as well as provides
// DefaultClient. Most non-testing code outside lib/kafka should not import
// these subpackages directly.
package kafka
