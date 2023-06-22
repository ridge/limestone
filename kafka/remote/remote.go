// Package remote is a Kafka client implementation that connects to a remote
// Kafka debug server.
//
// For safety reasons, this implementation is currently read-only.
package remote

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/ridge/limestone/kafka/api"
)

type client struct {
	httpClient *http.Client
	origin     string
}

// New returns a new remote Kafka client.
//
// baseURL is the server's HTTP origin (scheme://host:port)
func New(httpClient *http.Client, origin string) api.Client {
	if !strings.HasPrefix(origin, "http:") && !strings.HasPrefix(origin, "https:") {
		panic(errors.New("origin must have http: or https: scheme"))
	}
	return client{httpClient: httpClient, origin: origin}
}

func (c client) Write(ctx context.Context, topic string, messages []api.Message) error {
	panic("Remote Kafka client does not support writing")
}
