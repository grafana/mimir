// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"

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

// estimateCoverage says which blocks a cardinality estimate counted.
type estimateCoverage struct {
	// Missing holds the blocks no store-gateway counted after every attempt.
	Missing []ulid.ULID
	// Awaited holds the uncounted blocks of estimatePart.awaited.
	Awaited       []ulid.ULID
	StoreGateways int
	// Retries is the number of attempts after the first.
	Retries int
	// Failure is the last error of a store-gateway call whose blocks were left to another store-gateway.
	Failure error
}

// runSeriesCountsCalls sends every block in parts to a store-gateway that holds it, through do, and retries the uncounted blocks on store-gateways not yet tried, as the Series path does. A store-gateway whose call fails counts as not having counted its blocks; a context error, a refusal and a bad request fail the request.
//
// do returns the blocks the call counted, and must merge a call's result only when it returns no error, so a retried call is not counted twice.
func (q *BlocksStoreQueryable) runSeriesCountsCalls(
	ctx context.Context,
	tenantID string,
	parts []estimatePart,
	do func(ctx context.Context, c estimateCall) ([]ulid.ULID, error),
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

		results := make([][]ulid.ULID, len(calls))
		var failureMtx sync.Mutex
		g, gCtx := errgroup.WithContext(ctx)
		for i, c := range calls {
			gateways[c.client.RemoteAddress()] = struct{}{}
			g.Go(func() error {
				callCtx := grpcContextWithBucketStoreRequestMeta(gCtx, tenantID, c.part.indexMeta)
				res, err := do(callCtx, c)
				if err != nil {
					if shouldRetrySeriesCounts(gCtx, err) {
						// Its blocks are retried on another store-gateway while one is left.
						level.Warn(logger).Log("msg", "cardinality estimate: store-gateway call failed, its blocks are retried on another one", "store_gateway", c.client.RemoteAddress(), "attempt", attempt, "blocks", len(c.ids), "err", estimateFailureDetail(err))
						failureMtx.Lock()
						cov.Failure = err
						failureMtx.Unlock()
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
			asked := make(map[ulid.ULID]struct{}, len(c.ids))
			for _, id := range c.ids {
				attempted[id] = append(attempted[id], addr)
				asked[id] = struct{}{}
			}
			for _, id := range results[i] {
				// A block the call was not asked for is not taken as counted: another call may hold it.
				if _, ok := asked[id]; ok {
					counted[id] = true
				}
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
			if _, ok := parts[i].awaited[b.ID]; ok {
				cov.Awaited = append(cov.Awaited, b.ID)
				continue
			}
			cov.Missing = append(cov.Missing, b.ID)
		}
	}
	if len(cov.Missing) > 0 {
		level.Warn(logger).Log("msg", "cardinality estimate: blocks left uncounted after the last attempt", "attempts", cov.Retries+1, "blocks", strings.Join(convertULIDsToString(cov.Missing), ","))
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

// shouldRetrySeriesCounts reports whether a failed call's blocks are asked of another store-gateway.
// As on the Series path, a store-gateway's failure is retried whatever its gRPC code. A context error
// is not, nor a refusal or a bad request, which another store-gateway would answer the same way.
func shouldRetrySeriesCounts(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var e estimateError
	if errors.As(err, &e) {
		return e.class == estimateBusy
	}
	var failure redactedError
	return errors.As(err, &failure)
}

// errNothingCounted is the error for a request whose blocks no store-gateway counted.
func errNothingCounted(blocks int, cov estimateCoverage) error {
	if cov.Failure != nil {
		return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway: %w", blocks, cov.Failure)
	}
	return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway", blocks)
}
