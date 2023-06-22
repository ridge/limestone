package converter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"

	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"golang.org/x/oauth2/google"
	"time"
)

type activeSet map[string]map[string]*wire.ActiveObject // kind -> ID -> object

type surviveFn func(object wire.ActiveObject, now time.Time) bool

func (as activeSet) applyTxn(txn wire.Transaction, now time.Time, survive surviveFn) {
	for kind, kindChanges := range txn.Changes {
		byID := as[kind]
		if byID == nil {
			byID = map[string]*wire.ActiveObject{}
			as[kind] = byID
		}
		for id, diff := range kindChanges {
			obj := byID[id]
			if obj == nil {
				if _, ok := diff["ID"]; !ok {
					continue // update to a previously deleted object
				}
				obj = &wire.ActiveObject{
					Kind:  kind,
					TS:    *txn.TS,
					Props: wire.Diff{},
				}
				byID[id] = obj
			} else {
				obj.TS = *txn.TS
			}
			for k, v := range diff {
				obj.Props[k] = v
			}
			if !survive(*obj, now) {
				delete(byID, id)
			}
		}
	}
}

func parseGSURL(gsURL string) (bucket, object string, err error) {
	u, err := url.Parse(gsURL)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "gs" {
		return "", "", errors.New("unexpected URL scheme")
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

// recommended by Cloud Storage documentation
var gsRetryConfig = retry.ExpConfig{
	Min:     time.Second,
	Max:     time.Minute,
	Scale:   2,
	Instant: true,
}

func uploadActiveSet(ctx context.Context, gsURL string, header wire.ActiveSetHeader, as activeSet) error {
	bucket, object, err := parseGSURL(gsURL)
	if err != nil {
		return fmt.Errorf("failed to upload active set: %w", err)
	}

	err = retry.Do(ctx, gsRetryConfig, func() error {
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_write")
			if err != nil {
				return err
			}

			r, w := io.Pipe()

			spawn("post", parallel.Exit, func(ctx context.Context) error {
				u := "https://storage.googleapis.com/upload/storage/v1/b/" + url.QueryEscape(bucket) + "/o?" + url.Values{
					"name":       {object},
					"uploadType": {"media"},
				}.Encode()
				resp, err := client.Do(must.OK1(http.NewRequestWithContext(ctx, http.MethodPost, u, r)))
				must.OK(r.Close())
				if err != nil {
					var netErr net.Error
					if errors.As(err, &netErr) && netErr.Timeout() || errors.Is(err, syscall.ECONNRESET) {
						return retry.Retriable(err)
					}
					return err
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					err := fmt.Errorf("POST %s: %s", u, resp.Status)
					if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
						return retry.Retriable(err)
					}
					return err
				}
				return nil
			})

			spawn("data", parallel.Continue, func(ctx context.Context) error {
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				if err := enc.Encode(header); err != nil {
					return err
				}
				for _, byID := range as {
					for _, ao := range byID {
						if err := enc.Encode(*ao); err != nil {
							return err
						}
					}
				}
				return w.Close()
			})

			return nil
		})
	})

	if err != nil || !errors.Is(err, ctx.Err()) {
		return fmt.Errorf("failed to upload active set: %w", err)
	}
	return err
}

func downloadActiveSet(ctx context.Context, gsURL string, survive surviveFn) (header wire.ActiveSetHeader, as activeSet, err error) {
	bucket, object, err := parseGSURL(gsURL)
	if err != nil {
		return header, nil, fmt.Errorf("failed to download active set: %w", err)
	}
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_only")
	if err != nil {
		return header, nil, fmt.Errorf("failed to download active set: %w", err)
	}
	u := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
		url.QueryEscape(bucket), url.QueryEscape(object))

	err = retry.Do(ctx, gsRetryConfig, func() error {
		resp, err := client.Do(must.OK1(http.NewRequestWithContext(ctx, http.MethodGet, u, nil)))
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() || errors.Is(err, syscall.ECONNRESET) {
				return retry.Retriable(err)
			}
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("GET %s: %s", u, resp.Status)
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
				return retry.Retriable(err)
			}
			return err
		}

		dec := json.NewDecoder(resp.Body)

		if err := dec.Decode(&header); err != nil {
			if errors.Is(err, syscall.ECONNRESET) {
				return retry.Retriable(err)
			}
			return err
		}

		as = activeSet{}
		now := time.Now()
		for {
			var ao wire.ActiveObject
			err := dec.Decode(&ao)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, syscall.ECONNRESET) {
				return retry.Retriable(err)
			}
			if err != nil {
				return err
			}
			if !survive(ao, now) {
				continue
			}
			byID := as[ao.Kind]
			if byID == nil {
				byID = map[string]*wire.ActiveObject{}
				as[ao.Kind] = byID
			}
			var id string
			if err := json.Unmarshal(ao.Props["ID"], &id); err != nil {
				return err
			}
			if byID[id] != nil {
				return fmt.Errorf("duplicate %s %s", ao.Kind, id)
			}
			byID[id] = &ao
		}
	})

	if err != nil || !errors.Is(err, ctx.Err()) {
		return header, nil, fmt.Errorf("failed to download active set: %w", err)
	}
	return header, as, err
}
