package compactor

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type PerSeriesRetainerConfig struct {
}

type PerSeriesRetainer struct {
	services.Service

	cfg          PerSeriesRetainerConfig
	cfgProvider  ConfigProvider
	logger       log.Logger
	bucketClient objstore.Bucket
	usersScanner *mimir_tsdb.UsersScanner
	ownUser      func(userID string) (bool, error)
	singleFlight *concurrency.LimitedConcurrencySingleFlight
}

func NewPerSeriesRetainer(cfg PerSeriesRetainerConfig, bucketClient objstore.Bucket, ownUser func(userID string) (bool, error), cfgProvider ConfigProvider, logger log.Logger, reg prometheus.Registerer) *PerSeriesRetainer {
	r := &PerSeriesRetainer{
		cfg:          cfg,
		bucketClient: bucketClient,
		usersScanner: mimir_tsdb.NewUsersScanner(bucketClient, ownUser, logger),
		ownUser:      ownUser,
		cfgProvider:  cfgProvider,
		singleFlight: concurrency.NewLimitedConcurrencySingleFlight(1),
		logger:       log.With(logger, "component", "retainer"),
	}
	return r
}

func (c *PerSeriesRetainer) runCleanup(ctx context.Context, async bool) {
	// Wrap logger with some unique ID so if runCleanUp does run in parallel with itself, we can
	// at least differentiate the logs in this function for each run.
	// logger := log.With(c.logger, "run_id", strconv.FormatInt(time.Now().Unix(), 10))

	allUsers, _, _ := c.usersScanner.ScanUsers(ctx)

	doRetain := func() {
		c.retainTenants(ctx, allUsers)
	}

	if async {
		go doRetain()
	} else {
		doRetain()
	}
}

// cleanUsers must be concurrency-safe because some invocations may take longer and overlap with the next periodic invocation.
func (c *PerSeriesRetainer) retainTenants(ctx context.Context, allUsers []string) error {
	return c.singleFlight.ForEachNotInFlight(ctx, allUsers, func(ctx context.Context, userID string) error {
		own, err := c.ownUser(userID)
		if err != nil || !own {
			// This returns error only if err != nil. ForEachUser keeps working for other users.
			return errors.Wrapf(err, "check own user")
		}
		return errors.Wrapf(c.retainTenant(ctx, userID), "failed to retain blocks for user: %s", userID)
	})
}

func (c *PerSeriesRetainer) retainTenant(ctx context.Context, userID string) (returnErr error) {
	userLogger := util_log.WithUserID(userID, c.logger)
	userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
	startTime := time.Now()

	level.Info(userLogger).Log("msg", "started blocks retention")
	defer func() {
		if returnErr != nil {
			level.Warn(userLogger).Log("msg", "failed blocks retention", "err", returnErr)
		} else {
			level.Info(userLogger).Log("msg", "completed blocks retention", "duration", time.Since(startTime))
		}
	}()

	// Read the bucket index.
	idx, err := bucketindex.ReadIndex(ctx, c.bucketClient, userID, c.cfgProvider, c.logger)
	if errors.Is(err, bucketindex.ErrIndexCorrupted) {
		level.Warn(userLogger).Log("msg", "found a corrupted bucket index, recreating it")
	} else if err != nil && !errors.Is(err, bucketindex.ErrIndexNotFound) {
		return err
	}

	// Generate an updated in-memory version of the bucket index.
	w := bucketindex.NewUpdater(c.bucketClient, userID, c.cfgProvider, c.logger)
	idx, _, err = w.UpdateIndex(ctx, idx)
	if err != nil {
		return err
	}

	c.perSeriesRetention(ctx, idx, userBucket, userLogger)

	// Upload the updated index to the storage.
	if err := bucketindex.WriteIndex(ctx, c.bucketClient, userID, c.cfgProvider, idx); err != nil {
		return err
	}

	return nil
}

func (c *PerSeriesRetainer) perSeriesRetention(ctx context.Context, idx *bucketindex.Index, userBucket objstore.InstrumentedBucket, logger log.Logger) {

}
