package run

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ridge/limestone/tlog"
	"go.uber.org/zap"
)

func handleSignals(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		tlog.Get(ctx).Info("received signal, terminating", zap.Stringer("signal", sig))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
