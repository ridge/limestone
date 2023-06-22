package run

import (
	//
	// This package bundles CA certificates for use in TLS connections.
	//
	// Useful for empty containers.
	//
	// This import is placed into this package because every
	// binary we build to use in containers imports it.
	//
	_ "golang.org/x/crypto/x509roots/fallback"
)
