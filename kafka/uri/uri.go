package uri

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/ridge/limestone/kafka/api"
	"github.com/ridge/limestone/kafka/kafkago"
	"github.com/ridge/limestone/kafka/local"
	"github.com/ridge/limestone/kafka/remote"
)

var brokerRE = regexp.MustCompile(`^[-.a-z0-9]+:\d+$`)

// FromURI creates a client from an URI in one of the following formats:
//
// kafka://broker1:port1,broker2:port2,...
//
//	A real Kafka client that uses the specified brokers.
//
// file:///path
//
//	Local file-based implementation using the specified directory.
//
// http://server[:port] or https://server[:port]
//
//	Remote debug server (read-only)
func FromURI(uri string) (api.Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client for URI %s: %w", uri, err)
	}

	switch u.Scheme {
	case "kafka":
		if u.Path != "" {
			return nil, fmt.Errorf("failed to create Kafka client for URI %s: kafka://server1:port1,server2:port2... expected", uri)
		}
		brokers := strings.Split(u.Host, ",")
		for _, b := range brokers {
			if !brokerRE.MatchString(b) {
				return nil, fmt.Errorf("failed to create Kafka client for URI %s: kafka://server1:port1,server2:port2... expected", uri)
			}
		}
		return kafkago.New(brokers), nil

	case "http":
		if u.Path != "" {
			return nil, fmt.Errorf("failed to create Kafka client for URI %s: http://server[:port] or https://server[:port] expected", uri)
		}
		return remote.New(http.DefaultClient, uri), nil

	case "file":
		if u.Path == "" { // typical mistake: file://path instead of file:///path
			return nil, fmt.Errorf("failed to create Kafka client for URI %s: file:///... expected", uri)
		}
		client, err := local.New(u.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client for URI %s: %w", uri, err)
		}
		return client, nil

	default:
		return nil, fmt.Errorf("failed to create Kafka client for URI %s: kafka://..., http://..., https://... or file:///... expected", uri)
	}
}
