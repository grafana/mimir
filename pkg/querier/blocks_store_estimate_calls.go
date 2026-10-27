// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// estimateCall is one store-gateway call of a cardinality estimate: the
// blocks of one compartment that one store-gateway is asked to count.
type estimateCall struct {
	part   estimatePart
	client BlocksStoreClient
	ids    []ulid.ULID
}

// estimateCallResult is what a call reports back to runEstimateCalls: the
// blocks the store-gateway counted, and the index bytes it read.
type estimateCallResult struct {
	counted    []ulid.ULID
	indexBytes int64
}

// estimateCoverage says which blocks a cardinality estimate counted.
type estimateCoverage struct {
	// Missing holds the blocks that no store-gateway counted after every
	// attempt. When it is not empty the counts are a lower bound.
	Missing       []ulid.ULID
	StoreGateways int
	// Retries is the number of attempts after the first.
	Retries int
	// Unavailable is the last error from a store-gateway that could not
	// be reached, kept so a request that counted nothing can say why.
	Unavailable error
}

// errEstimateBudgetSpent is returned by runEstimateCalls when a retry is
// needed but the request's max_index_bytes is already spent.
var errEstimateBudgetSpent = errors.New("max_index_bytes is spent before every block was counted")

// runEstimateCalls sends every block in parts to a store-gateway that holds
// it, through do, and retries the blocks that came back uncounted on
// store-gateways not yet tried, up to the largest replication factor, as
// the Series path does. A store-gateway that is unavailable counts as not
// having counted its blocks, so those are retried too; any other error
// fails the request.
//
// maxIndexBytes, when positive, is split evenly across the calls of each
// attempt from what earlier attempts left unread, so the request as a whole
// reads at most that many bytes.
//
// do must merge a call's result into the caller's state only when it
// returns no error, so a failed call that is retried is not counted twice.
func (q *BlocksStoreQueryable) runEstimateCalls(
	ctx context.Context,
	tenantID string,
	parts []estimatePart,
	maxIndexBytes int64,
	do func(ctx context.Context, c estimateCall, maxIndexBytes int64, calls int) (estimateCallResult, error),
) (estimateCoverage, error) {
	logger := util_log.WithContext(ctx, q.estimateLogger())
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
				// On a retry this means no store-gateway is left to try
				// for these blocks, which stay uncounted.
				if attempt > 1 {
					level.Warn(logger).Log("msg", "cardinality estimate: no store-gateway left to count blocks, they are left out and the answer is a lower bound", "attempt", attempt, "blocks", len(remaining[i]), "err", err)
					continue
				}
				// The ring's error names the store-gateway's address, which
				// the caller must not see; the request's log line has it.
				return estimateCoverage{}, redactFailure("the store-gateway client lookup", err)
			}
			for client, partitions := range clients {
				for _, ids := range partitions {
					calls = append(calls, estimateCall{part: p, client: client, ids: ids})
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
				return estimateCoverage{}, refusedByLimit(refusedByMaxIndexBytes, "%w (max_index_bytes %d)", errEstimateBudgetSpent, maxIndexBytes)
			}
			perCall = splitIndexBytes(left, len(calls))
		}

		results := make([]estimateCallResult, len(calls))
		var unavailableMtx sync.Mutex
		g, gCtx := errgroup.WithContext(ctx)
		for i, c := range calls {
			gateways[c.client.RemoteAddress()] = struct{}{}
			g.Go(func() error {
				callCtx := grpcContextWithBucketStoreRequestMeta(gCtx, tenantID, c.part.indexMeta)
				res, err := do(callCtx, c, perCall, len(calls))
				if err != nil {
					if isStoreGatewayUnavailable(err) {
						// Its blocks are retried on another store-gateway
						// while one is left, as the Series path does.
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

// estimateLogger is the queryable's logger, or a no-op one when it has none.
func (q *BlocksStoreQueryable) estimateLogger() log.Logger {
	if q.logger == nil {
		return log.NewNopLogger()
	}
	return q.logger
}

// isStoreGatewayUnavailable reports whether err means the store-gateway
// could not be reached, so its blocks can be asked of another one.
func isStoreGatewayUnavailable(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := status.FromError(e); ok && st.Code() == codes.Unavailable {
			return true
		}
	}
	return false
}

// errNothingCounted is the error for a request whose blocks no
// store-gateway counted, with the last unavailable store-gateway's error
// when there was one.
func errNothingCounted(blocks int, cov estimateCoverage) error {
	if cov.Unavailable != nil {
		return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway: %w", blocks, cov.Unavailable)
	}
	return fmt.Errorf("none of the %d blocks for this window was counted by a store-gateway", blocks)
}
