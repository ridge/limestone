package server

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
	"time"

	"github.com/ridge/limestone/retry"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/must/v2"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
)

// FIXME (alexey): Fine-tune this
const objectsPerMessage = 256

// recommended by Cloud Storage documentation
var gsRetryConfig = retry.ExpConfig{
	Min:     time.Second,
	Max:     time.Minute,
	Scale:   2,
	Instant: true,
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

func hotStartMessage(kind string, changes wire.KindChanges) []byte {
	txn := wire.IncomingTransaction{Transaction: wire.Transaction{Changes: wire.Changes{kind: changes}}}
	return must.OK1(json.Marshal(wire.Notification{Txn: &txn}))
}

func (s *server) pullHotStart(ctx context.Context, gsURL string) error {
	bucket, object, err := parseGSURL(gsURL)
	if err != nil {
		return fmt.Errorf("failed to download hot start data: %w", err)
	}
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_only")
	if err != nil {
		return fmt.Errorf("failed to download hot start data: %w", err)
	}
	u := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
		url.QueryEscape(bucket), url.QueryEscape(object))
	tlog.Get(ctx).Info("Downloading hot start data", zap.String("url", gsURL))

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
		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusNotFound: // No hot start data: this is not an error
			tlog.Get(ctx).Info("Hot start data not found", zap.String("url", gsURL))
			return nil
		default:
			err := fmt.Errorf("GET %s: %s", u, resp.Status)
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
				return retry.Retriable(err)
			}
			return err
		}

		dec := json.NewDecoder(resp.Body)

		var header wire.ActiveSetHeader
		if err := dec.Decode(&header); err != nil {
			if errors.Is(err, syscall.ECONNRESET) {
				return retry.Retriable(err)
			}
			return err
		}

		if header.Version != s.version {
			return fmt.Errorf("expected version %d, found %d", s.version, header.Version)
		}

		messages := map[string][][]byte{}
		current := wire.Changes{}
		objects := 0

		for {
			var ao wire.ActiveObject
			err := dec.Decode(&ao)
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, syscall.ECONNRESET) {
				return retry.Retriable(err)
			}
			if err != nil {
				return err
			}

			byID := current[ao.Kind]
			if byID == nil {
				byID = wire.KindChanges{}
				current[ao.Kind] = byID
			}
			var id string
			must.OK(json.Unmarshal(ao.Props["ID"], &id))
			byID[id] = ao.Props
			objects++

			if len(current[ao.Kind]) == objectsPerMessage {
				messages[ao.Kind] = append(messages[ao.Kind], hotStartMessage(ao.Kind, current[ao.Kind]))
				delete(current, ao.Kind)
			}
		}
		for kind, changes := range current {
			messages[kind] = append(messages[kind], hotStartMessage(kind, changes))
		}

		s.hotStart = messages
		s.hotStartPos = header.Position
		tlog.Get(ctx).Info("Hot start data downloaded", zap.String("url", gsURL), zap.Int("objects", objects))
		return nil
	})

	if err != nil || !errors.Is(err, ctx.Err()) {
		return fmt.Errorf("failed to download hot start data: %w", err)
	}
	return err
}
