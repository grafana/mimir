// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/util"
)

const (
	// ClusterSeedFileName is the file name for the cluster seed file.
	ClusterSeedFileName = "mimir_cluster_seed.json"

	// clusterSeedFileMinStability is how long to wait for a cluster seed file creation
	// before using it (because multiple replicas could concurrently create it).
	clusterSeedFileMinStability = 5 * time.Minute
)

var (
	errClusterSeedFileCorrupted = errors.New("the cluster seed file is corrupted")
)

// ClusterSeed is the seed for the usage stats.
type ClusterSeed struct {
	// UID is the unique cluster ID.
	UID string `json:"UID"`

	// CreatedAt is the timestamp when the seed file was created.
	CreatedAt time.Time `json:"created_at"`
}

func newClusterSeed() ClusterSeed {
	return ClusterSeed{
		UID:       uuid.NewString(),
		CreatedAt: time.Now(),
	}
}

// initSeedFile returns the cluster seed, creating it if required. This function returns only after the seed
// file is created and stable, or context has been canceled.
func initSeedFile(ctx context.Context, bucket objstore.InstrumentedBucket, minStability time.Duration, logger log.Logger) (ClusterSeed, error) {
	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: time.Second,
		MaxBackoff: time.Minute,
		MaxRetries: 0,
	})

	for backoff.Ongoing() {
		// Check if the seed file is already created and wait for its stability.
		seed, err := waitSeedFileStability(ctx, bucket, minStability, logger)
		if err == nil {
			return seed, nil
		}

		// Either the seed file does not exist or it's corrupted. Before attempting to write it
		// we want to wait some time to reduce the likelihood multiple Mimir replicas will attempt
		// to create it at the same time.
		select {
		case <-time.After(util.DurationWithJitter(minStability/2, 1)): // Wait between 0s and minStability
		case <-ctx.Done():
			return ClusterSeed{}, ctx.Err()
		}

		// Ensure the seed file hasn't been created in the meanwhile.
		// If so, we should get back to wait for its stability.
		if seed, err := readSeedFile(ctx, bucket, logger); err == nil {
			level.Debug(logger).Log("msg", "skipping creation of cluster seed file because found one", "cluster_id", seed.UID, "created_at", seed.CreatedAt.String())
			backoff.Wait()
			continue
		}

		// Create or re-create the seed file.
		seed = newClusterSeed()
		level.Info(logger).Log("msg", "creating cluster seed file", "cluster_id", seed.UID)

		if err := writeSeedFile(ctx, bucket, seed); err != nil {
			level.Warn(logger).Log("msg", "failed to create cluster seed file", "err", err)
		}

		// Either on success of failure we need to loop back to wait for stability. Why?
		// If creation succeeded, other replicas may have concurrently tried to create it, so
		// we need to wait for its stability.
		// If creation failed, other replicas may have succeeded, so we get back to check it.
		backoff.Wait()
	}

	return ClusterSeed{}, backoff.Err()
}

// waitSeedFileStability reads the seed file from the object storage and waits until it was created at least minStability
// time ago. Returns an error if the seed file doesn't exist, or it's corrupted, or context is canceled.
func waitSeedFileStability(ctx context.Context, bucket objstore.InstrumentedBucket, minStability time.Duration, logger log.Logger) (ClusterSeed, error) {
	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: time.Second,
		MaxBackoff: time.Minute,
		MaxRetries: 0,
	})

	for backoff.Ongoing() {
		seed, err := readSeedFile(ctx, bucket, logger)
		if bucket.IsObjNotFoundErr(err) {
			// The seed file doesn't exist. We'll attempt to create it.
			return ClusterSeed{}, err
		}
		if errors.Is(err, errClusterSeedFileCorrupted) {
			// The seed file doesn't exist. We'll attempt to re-create it.
			return ClusterSeed{}, err
		}
		if err != nil {
			// We assume any other error to either be a transient error (e.g. network error)
			// or a permanent error which won't be fixed trying to re-create the seed file (e.g. auth error).
			// Since there's no reliable way to detect whether an error is transient or permanent,
			// we just keep retrying.
			level.Warn(logger).Log("msg", "failed to read cluster seed file from object storage", "err", err)
			backoff.Wait()
			continue
		}

		// Check if stability has been reached. We assume clock is synchronized between Mimir replicas
		// (this is an assumption done in other Mimir logic too).
		createdTimeAgo := time.Since(seed.CreatedAt)
		if createdTimeAgo >= minStability {
			return seed, nil
		}

		// Wait until the min stability should have been reached. We add an extra jitter: in case there are many
		// replicas they will not look up the seed file at the same time.
		level.Info(logger).Log("msg", "found a cluster seed file created recently, waiting until stable", "cluster_id", seed.UID, "created_at", seed.CreatedAt.String())
		select {
		case <-time.After(util.DurationWithPositiveJitter(minStability-createdTimeAgo, 0.2)):
		case <-ctx.Done():
			return ClusterSeed{}, ctx.Err()
		}
	}

	return ClusterSeed{}, backoff.Err()
}

// readSeedFile reads the cluster seed file from the object store.
func readSeedFile(ctx context.Context, bucket objstore.InstrumentedBucket, logger log.Logger) (ClusterSeed, error) {
	reader, err := bucket.WithExpectedErrs(bucket.IsObjNotFoundErr).Get(ctx, ClusterSeedFileName)
	if err != nil {
		return ClusterSeed{}, err
	}

	// Ensure the reader will be closed.
	defer runutil.CloseWithLogOnErr(logger, reader, "failed to close cluster seed reader")

	data, err := io.ReadAll(reader)
	if err != nil {
		return ClusterSeed{}, err
	}

	var seed ClusterSeed
	if err := json.Unmarshal(data, &seed); err != nil {
		return ClusterSeed{}, errors.Wrap(errClusterSeedFileCorrupted, err.Error())
	}

	return seed, nil
}

// writeSeedFile writes the cluster seed to the object store.
func writeSeedFile(ctx context.Context, bucket objstore.InstrumentedBucket, seed ClusterSeed) error {
	data, err := json.Marshal(seed)
	if err != nil {
		return err
	}

	return bucket.Upload(ctx, ClusterSeedFileName, bytes.NewReader(data))
}
