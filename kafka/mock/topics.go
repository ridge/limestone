package mock

import (
	"context"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// Topics implements the Topics method of the kafka.Client interface (see
// documentation) by enumerating the topics in the simulated database
func (k *kafka) Topics(ctx context.Context) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	topicsKeys := maps.Keys(k.topics)
	slices.Sort(topicsKeys)

	return topicsKeys, nil
}
