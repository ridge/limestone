package converter

import (
	"context"
	"errors"
	"fmt"

	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/converter/xform"
	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/tlog"
	"github.com/ridge/limestone/wire"
	"go.uber.org/zap"
	"time"
)

// Config describes the converter configuration
type Config struct {
	SourceKafka      kafka.Client
	DestKafka        kafka.Client
	NewTopic         string // output topic name
	Force            bool   // perform a null conversion even if the database version is current
	DryRun           bool   // do not publish new manifest
	SafetyInterval   time.Duration
	HotStartStorage  string
	DBVersionHistory []string
	UpgradeSteps     map[string]xform.Transformation
	Survive          func(object wire.ActiveObject, now time.Time) bool
}

// DatabaseIsEmpty returns true if database is empty
func DatabaseIsEmpty(ctx context.Context, config Config) (bool, error) {
	lc := client.NewKafkaClient(config.SourceKafka)

	_, err := lc.RetrieveManifest(ctx, false)
	if errors.Is(err, client.ErrNoManifest) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return false, nil
}

// Run runs the converter
func Run(ctx context.Context, config Config) error {
	logger := tlog.Get(ctx)
	lc := client.NewKafkaClient(config.SourceKafka)

	manifest, err := lc.RetrieveManifest(ctx, true)
	if err != nil {
		return err
	}
	logger.Info("Retrieved manifest", zap.Object("manifest", manifest))

	if manifest.Version > len(config.DBVersionHistory) {
		panic(fmt.Errorf("actual database version %d is later than target %d", manifest.Version, len(config.DBVersionHistory)))
	}

	if manifest.Version == len(config.DBVersionHistory) && !config.Force {
		if config.HotStartStorage != "" {
			logger.Info("Database already at target version, refreshing hot start data", zap.Int("manifestVersion", manifest.Version))
			return refreshHotStart(ctx, config, lc, manifest)
		}
		logger.Info("Database already at target version, nothing to do", zap.Int("manifestVersion", manifest.Version))
		return nil
	}

	logger.Info("Preparing to upgrade", zap.Int("fromVersion", manifest.Version), zap.Int("toVersion", len(config.DBVersionHistory)))
	var steps []step
	for ver := manifest.Version; ver < len(config.DBVersionHistory); ver++ {
		name := config.DBVersionHistory[ver]
		run := config.UpgradeSteps[name]
		if run == nil {
			panic(fmt.Errorf("upgrade step %s from version %d to %d is not available", name, ver, ver+1))
		}
		steps = append(steps, step{name: name, run: run})
	}

	return transform(ctx, config, lc, manifest, steps)
}
