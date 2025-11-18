// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/usagetracker"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

const (
	defaultIdleTimeout                         = 20 * time.Minute
	defaultUserCloseToLimitPercentageThreshold = 85
	defaultSleepInterval                       = 5 * time.Second
)

func main() {
	var (
		snapshotFile                        = flag.String("snapshot-file", "", "Path to the snapshot file to load")
		sleepInterval                       = flag.Duration("sleep-interval", defaultSleepInterval, "Time to sleep between load iterations")
		idleTimeout                         = flag.Duration("idle-timeout", defaultIdleTimeout, "Idle timeout for the tracker store")
		userCloseToLimitPercentageThreshold = flag.Int("user-close-to-limit-threshold", defaultUserCloseToLimitPercentageThreshold, "User close to limit percentage threshold")
	)
	flag.Parse()

	if *snapshotFile == "" {
		fmt.Fprintf(os.Stderr, "Error: -snapshot-file is required\n")
		flag.Usage()
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	now := time.Now()
	iteration := 0

	for {
		if iteration > 0 {
			time.Sleep(*sleepInterval)
		}

		iteration++
		fileData, err := os.ReadFile(*snapshotFile)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to read snapshot file", "file", *snapshotFile, "err", err)
			continue
		}

		var file usagetrackerpb.SnapshotFile
		if err := file.Unmarshal(fileData); err != nil {
			level.Error(logger).Log("msg", "Failed to unmarshal snapshot file", "file", *snapshotFile, "err", err)
			continue
		}

		if len(file.Data) == 0 {
			level.Error(logger).Log("msg", "Snapshot file contains no shard data", "file", *snapshotFile)
			continue
		}

		level.Info(logger).Log("msg", "Starting snapshot load iteration", "iteration", iteration, "file", *snapshotFile, "shards", len(file.Data), "sleep_interval", *sleepInterval)

		loadStart := time.Now()

		if err := usagetracker.LoadSnapshotsForTool(file.Data, *idleTimeout, *userCloseToLimitPercentageThreshold, logger, now); err != nil {
			level.Error(logger).Log("msg", "Failed to load snapshots", "iteration", iteration, "err", err)
		} else {
			loadDuration := time.Since(loadStart)
			level.Info(logger).Log("msg", "Loaded snapshots", "iteration", iteration, "duration", loadDuration)
		}
	}
}
