// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_consistency_checker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

type BlocksConsistency struct {
	uploadGracePeriod time.Duration

	checksTotal  prometheus.Counter
	checksFailed prometheus.Counter
}

func NewBlocksConsistency(uploadGracePeriod time.Duration, reg prometheus.Registerer) *BlocksConsistency {
	return &BlocksConsistency{
		uploadGracePeriod: uploadGracePeriod,
		checksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_blocks_consistency_checks_total",
			Help: "Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.",
		}),
		checksFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_blocks_consistency_checks_failed_total",
			Help: "Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.",
		}),
	}
}

// NewTracker creates a consistency tracker from the known blocks. It filters out any block uploaded within uploadGracePeriod
// and with a deletion mark within deletionGracePeriod.
func (c *BlocksConsistency) NewTracker(knownBlocks bucketindex.Blocks, logger log.Logger) BlocksConsistencyTracker {
	blocksToTrack := make(map[ulid.ULID]struct{}, len(knownBlocks))
	for _, block := range knownBlocks {
		// Some recently uploaded blocks, already discovered by the querier, may not have been discovered
		// and loaded by the store-gateway yet. In order to avoid false positives, we grant some time
		// to the store-gateway to discover them. It's safe to exclude recently uploaded blocks because:
		// - Blocks uploaded by ingesters: we will continue querying them from ingesters for a while (depends
		//   on the configured retention period).
		// - Blocks uploaded by compactor: the source blocks are marked for deletion but will continue to be
		//   queried by queriers for a while (depends on the configured deletion marks delay).
		if c.uploadGracePeriod > 0 && time.Since(block.GetUploadedAt()) < c.uploadGracePeriod {
			level.Debug(logger).Log("msg", "block skipped from consistency check because it was uploaded recently; block will be queried at most once but not retried and failures will not cause the query to fail", "block", block.ID.String(), "uploadedAt", block.GetUploadedAt().String())
			continue
		}

		blocksToTrack[block.ID] = struct{}{}
	}

	return BlocksConsistencyTracker{
		checksFailed: c.checksFailed,
		checksTotal:  c.checksTotal,
		tracked:      blocksToTrack,
		queried:      make(map[ulid.ULID]struct{}, len(blocksToTrack)),
	}
}

type BlocksConsistencyTracker struct {
	checksTotal  prometheus.Counter
	checksFailed prometheus.Counter

	tracked map[ulid.ULID]struct{}
	queried map[ulid.ULID]struct{}
}

// Check takes a slice of blocks which can be all queried blocks so far or only blocks queried since the last call to Check.
// Check returns the blocks which haven't been seen in any call to Check yet.
func (c BlocksConsistencyTracker) Check(queriedBlocks []ulid.ULID) (missingBlocks []ulid.ULID) {
	// Make map of queried blocks, for quick lookup.
	for _, blockID := range queriedBlocks {
		if _, ok := c.tracked[blockID]; !ok {
			// Since we will use the length of c.queried to check if the consistency check was successful in the end,
			// we don't include blocks we weren't asked to track in the first place.
			continue
		}
		c.queried[blockID] = struct{}{}
	}

	// Look for any missing blocks.
	for block := range c.tracked {
		if _, ok := c.queried[block]; !ok {
			missingBlocks = append(missingBlocks, block)
		}
	}

	return missingBlocks
}

// Complete should be called once the request tracked by this BlocksConsistencyTracker has been completed.
// This function should NOT be called if the request is canceled or interrupted due to any error.
func (c BlocksConsistencyTracker) Complete() {
	c.checksTotal.Inc()

	if len(c.queried) < len(c.tracked) {
		c.checksFailed.Inc()
	}
}
