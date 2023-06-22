package mock

import "context"

func (k *kafka) LastOffset(ctx context.Context, topic string) (int64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	t := k.topics[topic]
	if t == nil {
		return 0, nil
	}

	return int64(len(t.data)), nil
}
