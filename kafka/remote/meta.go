package remote

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ridge/must/v2"
)

func (c client) Topics(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/kafka", c.origin)
	req := must.OK1(http.NewRequestWithContext(ctx, http.MethodGet, url, nil))
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve list of topics: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve list of topics: %w", err)
	}
	return strings.Split(strings.TrimSpace(string(body)), "\n"), nil
}

func (c client) LastOffset(ctx context.Context, topic string) (int64, error) {
	url := fmt.Sprintf("%s/kafka/%s", c.origin, topic)
	req := must.OK1(http.NewRequestWithContext(ctx, http.MethodHead, url, nil))
	res, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to check last offset for topic %s: %w", topic, err)
	}
	res.Body.Close()
	offset, err := strconv.ParseInt(res.Header.Get("X-Last-Offset"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to check last offset for topic %s: %w", topic, err)
	}
	return offset, nil
}
