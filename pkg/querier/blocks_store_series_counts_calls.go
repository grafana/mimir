// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// estimateCall is one store-gateway call of a cardinality estimate.
type estimateCall struct {
	part   estimatePart
	client BlocksStoreClient
	ids    []ulid.ULID
	// attempt is 1 for the first call of a block, higher on a retry.
	attempt int
}

// seriesCountsCallResult is what a call reports back to runSeriesCountsCalls.
type seriesCountsCallResult struct {
	counted    []ulid.ULID
	indexBytes int64
}

// estimateCoverage says which blocks a cardinality estimate counted.
type estimateCoverage struct {
	// Missing holds the blocks no store-gateway counted after every attempt.
	Missing       []ulid.ULID
	StoreGateways int
	// Retries is the number of attempts after the first.
	Retries int
	// Unavailable is the last error from a store-gateway that could not be reached.
	Unavailable error
}

// errSeriesCountsBudgetSpent is returned by runSeriesCountsCalls when a retry is needed but max_index_bytes is spent.
var errSeriesCountsBudgetSpent = errors.New("max_index_bytes is spent before every block was counted")

// runSeriesCountsCalls sends every block in parts to a store-gateway that holds it, through do, and retries the uncounted blocks on store-gateways not yet tried, as the Series path does. An unavailable store-gateway counts as not having counted its blocks; any other error fails the request.
//
// maxIndexBytes, when positive, is split evenly across the calls of each attempt from what earlier attempts left unread.
//
// do must merge a call's result only when it returns no error, so a retried call is not counted twice.
func (q *BlocksStoreQueryable) runSeriesCountsCalls(
	ctx context.Context,
	tenantID string,
	parts []estimatePart,
	maxIndexBytes int64,
	do func(ctx context.Context, c estimateCall, maxIndexBytes int64, calls int) (seriesCountsCallResult, error),
) (estimateCoverage, error) {
	logger := util_log.WithContext(ctx, q.seriesCountsLogger())
	maxAttempts := 1
	if q.dynamicReplication != nil {
		maxAttempts = max(1, q.dynamicReplication.MaxReplicationFactor())
	}

	var (
		cov       estimateCoverage
		gateways  = map[string]struct{}{}
		attempted = map[ulid.ULID][]string{}
		counted   = map[ulid.ULID]bool{}
		read      int64
		remaining = make([]bucketindex.Blocks, len(parts))
	)
	for i, p := range parts {
		remaining[i] = p.blocks
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var calls []estimateCall
		for i, p := range parts {
			if len(remaining[i]) == 0 {
				continue
			}
			clients, err := p.c.stores.GetClientsFor(tenantID, remaining[i], attempted)
			if err != nil {
				// No store-gateway is left to try for these blocks.
				if attempt > 1 {
					level.Warn(logger).Log("msg", "cardinality estimate: no store-gateway left to count blocks, they are left out and the answer is a lower bound", "attempt", attempt, "blocks", len(remaining[i]), "err", err)
					continue
				}
				// The ring's error names the store-gateway's address, which the caller must not see.
				return estimateCoverage{}, redactFailure("the store-gateway client lookup", err)
			}
			for client, partitions := range clients {
				for _, ids := range partitions {
					calls = append(calls, estimateCall{part: p, client: client, ids: ids, attempt: attempt})
				}
			}
		}
		if len(calls) == 0 {
			break
		}
		if attempt > 1 {
			cov.Retries++
		}

		perCall := int64(0)
		if maxIndexBytes > 0 {
			left := maxIndexBytes - read
			if left <= 0 {
				return estimateCoverage{}, refusedByLimit(refusedByMaxIndexBytes, "%w (max_index_bytes %d)", errSeriesCountsBudgetSpent, maxIndexBytes)
			}
			perCall = splitIndexBytes(left, len(calls))
		}

		results := make([]seriesCountsCallResult, len(calls))
		var unavailableMtx sync.Mutex
		g, gCtx := errgroup.WithContext(ctx)
		for i, c := range calls {
			gateways[c.client.RemoteAddress()] = struct{}{}
			g.Go(func() error {
				callCtx := grpcContextWithBucketStoreRequestMeta(gCtx, tenantID, c.part.indexMeta)
				res, err := do(callCtx, c, perCall, len(calls))
				if err != nil {
					if isStoreGatewayUnavailable(err) {
						// Its blocks are retried on another store-gateway while one is left.
						level.Warn(logger).Log("msg", "cardinality estimate: store-gateway unavailable, its blocks are retried on another one", "store_gateway", c.client.RemoteAddress(), "attempt", attempt, "blocks", len(c.ids), "err", estimateFailureDetail(err))
						unavailableMtx.Lock()
						cov.Unavailable = err
						unavailableMtx.Unlock()
						return nil
					}
					return err
				}
				results[i] = res
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return estimateCoverage{}, err
		}

		for i, c := range calls {
			addr := c.client.RemoteAddress()
			for _, id := range c.ids {
				attempted[id] = append(attempted[id], addr)
			}
			read += results[i].indexBytes
			for _, id := range results[i].counted {
				counted[id] = true
			}
		}

		left := 0
		for i, p := range parts {
			var still bucketindex.Blocks
			for _, b := range p.blocks {
				if !counted[b.ID] {
					still = append(still, b)
				}
			}
			remaining[i] = still
			left += len(still)
		}
		if left == 0 {
			break
		}
	}

	cov.StoreGateways = len(gateways)
	for i := range parts {
		for _, b := range remaining[i] {
			cov.Missing = append(cov.Missing, b.ID)
		}
	}
	return cov, nil
}

// seriesCountsLogger is the queryable's logger, or a no-op one when it has none.
func (q *BlocksStoreQueryable) seriesCountsLogger() log.Logger {
	if q.logger == nil {
		return log.NewNopLogger()
	}
	return q.logger
}

// isStoreGatewayUnavailable reports whether err means the store-gateway could not be reached.
func isStoreGatewayUnavailable(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := grpcutil.ErrorToStatus(e); ok && st.Code() == codes.Unavailable {
			return true
		}
	}
	return false
}

// errNothingCounted is the error for a request whose blocks no store-gateway counted.
func errNothingCounted(blocks int, cov estimateCoverage) error {
	if cov.Unavailable != nil {
		return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway: %w", blocks, cov.Unavailable)
	}
	return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway", blocks)
}
