package thttp

import (
	"context"
	"errors"
	"net"
	"net/http"

	"time"

	"github.com/ridge/limestone/retry"
)

var (
	defaultDialer               = net.Dialer{}
	retryingDialerBackoffConfig = retry.ExpConfig{
		Min:   10 * time.Millisecond,
		Max:   5 * time.Second,
		Scale: 1.5,
	}
)

// retryingDialer is a dialer for http.Transport that retries dialing if DNS
// record for given address does not exist
func retryingDialer(ctx context.Context, network, address string) (net.Conn, error) {
	backoff := retry.NewExpBackoff(retryingDialerBackoffConfig)

	for ctx.Err() == nil {
		conn, err := defaultDialer.DialContext(ctx, network, address)
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			// Sleep a bit: DNS "not found" errors take some time to be rectified
			t := time.NewTimer(backoff.Backoff())
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
			}
			continue
		}
		return conn, err
	}
	return nil, ctx.Err()
}

// RetryingDNSClient is a http.Client that retries in case of DNS "not found"
// errors
var RetryingDNSClient = &http.Client{
	Transport: &http.Transport{
		DialContext: retryingDialer,
	},
}
