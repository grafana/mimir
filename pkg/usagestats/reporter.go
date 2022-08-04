// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"context"
	"flag"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

type Config struct {
	Enabled bool `yaml:"enabled" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "usage-stats.enabled", false, "Enable anonymous usage reporting.")
}

type Reporter struct {
	logger log.Logger
	bucket objstore.InstrumentedBucket

	services.Service
}

func NewReporter(bucketClient objstore.InstrumentedBucket, logger log.Logger) *Reporter {
	// The cluster seed file is stored in a prefix dedicated to Mimir internals.
	bucketClient = bucket.NewPrefixedBucketClient(bucketClient, bucket.MimirInternalsPrefix)

	r := &Reporter{
		logger: logger,
		bucket: bucketClient,
	}
	r.Service = services.NewBasicService(nil, r.running, nil)
	return r
}

func (r *Reporter) running(ctx context.Context) error {
	// Init or get the cluster seed.
	seed, err := initSeedFile(ctx, r.bucket, clusterSeedFileMinStability, r.logger)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}

	level.Info(r.logger).Log("msg", "usage stats reporter initialized", "cluster_id", seed.UID)

	// TODO Periodically send usage report.

	return nil
}
