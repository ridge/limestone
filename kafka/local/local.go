// Package local is a local file-based implementation of kafka.Client. It stores
// data in append-only topic files a local directory. Several instances of
// local.Kafka communicate correctly via this directory even across process
// boundaries.
//
// The purpose is to eliminate the memory footprint and hassle of running real
// Kafka for local testing such as in a box environment.
//
// The implementation is not perfectly reliable: it can theoretically leave a
// topic file corrupted if a write succeeds partially. However, this is highly
// unlikely, so the implementation is good enough for local testing.
package local

import (
	"context"
	"fmt"
	"os"

	"github.com/ridge/limestone/kafka/api"
)

type client struct {
	dir string
}

// New creates a new Kafka instance based on the given directory name. The
// directory will be created along with its intermediate parents if it doesn't
// already exist.
func New(dir string) (api.ClientBackdate, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create local Kafka: %w", err)
	}
	return client{dir: dir}, nil
}

func (c client) Topics(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of topics: %w", err)
	}

	res := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Type().IsRegular() {
			res = append(res, e.Name())
		}
	}
	return res, nil
}
