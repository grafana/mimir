// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrSingleflightLeaderTimeout is returned to a follower when the leader's compute
// hits singleflightLeaderTimeout. It is distinguishable from the follower's own
// context.DeadlineExceeded so callers and gRPC error mappings can attribute the
// failure to the safety-net trip rather than treating it as the follower's own
// request budget elapsing.
var ErrSingleflightLeaderTimeout = errors.New("singleflight leader compute timeout")

// singleflightLeaderTimeout bounds the leader's compute lifetime so a stalled inner
// operation (memcached, object storage, index header reader) without its own timeout
// cannot leak the singleflight goroutine indefinitely. It is intentionally generous:
// the typical compute is sub-second; this is a safety net, not a request deadline.
//
// Boundary behaviour: when the leader trips this timeout, every follower currently
// waiting on the same key fails simultaneously with ErrSingleflightLeaderTimeout.
// Their callers then retry, electing a new leader; if the underlying cause is
// persistent (e.g. a genuinely stuck object-storage GET) the new leader times out
// again and the cycle repeats. In practice the timeout is large enough relative to
// real S3/GCS budgets (typically <1 minute) that this only fires under degenerate
// dependency stalls — but the failure mode is "all-followers-fail-then-retry-storm"
// rather than a graceful single-failure handoff.
const singleflightLeaderTimeout = 5 * time.Minute

// singleflightFetchOrCompute coalesces concurrent fetch+compute+store passes for the same key.
//
// On cache hit (the outer fetch returns ok=true), the call returns immediately without
// touching the singleflight group. On cache miss, the first goroutine per key (the
// "leader") runs the inner closure: re-fetch from the cache (defensive; another writer
// outside this group may have populated it) and otherwise call computeAndStore.
// Concurrent callers for the same key block on a shared channel and reuse the result.
//
// A caller whose context is cancelled while waiting for the result bails out with the
// context's error. The leader's compute is unaffected: it runs on a context derived via
// context.WithoutCancel(ctx), which inherits the originating request's values (trace
// span, tenant id, logger fields) without inheriting cancellation. A safety timeout
// (singleflightLeaderTimeout) bounds the compute so a stalled inner client cannot
// leak the goroutine indefinitely.
//
// Lifetime: computeAndStore is responsible for keeping any external resources it
// depends on alive for its own duration. The helper does not (and cannot, being
// generic over T) take that responsibility on behalf of the caller. For example, a
// caller computing against a refcounted block must acquire its own refcount inside
// computeAndStore rather than relying on the originating caller's refcount.
//
// Both fetch invocations must be cheap, or a deliberate no-op closure when the
// caller has no upstream cache to consult — readIndexRangeSF does this for
// byte-range fetches, where the only thing the helper provides is the
// concurrent-coalescing and leader-timeout error wrapping.
func singleflightFetchOrCompute[T any](
	ctx context.Context,
	sf *singleflight.Group,
	key string,
	fetch func(ctx context.Context) (T, bool),
	computeAndStore func(ctx context.Context) (T, error),
) (T, error) {
	if cached, ok := fetch(ctx); ok {
		return cached, nil
	}
	ch := sf.DoChan(key, func() (any, error) {
		sfCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleflightLeaderTimeout)
		defer cancel()
		if cached, ok := fetch(sfCtx); ok {
			return cached, nil
		}
		val, err := computeAndStore(sfCtx)
		if err != nil && errors.Is(sfCtx.Err(), context.DeadlineExceeded) {
			// Mark leader-side safety-net trips so followers (whose own contexts may
			// still be healthy) don't conflate this with their own deadline elapsing.
			err = fmt.Errorf("%w: %w", ErrSingleflightLeaderTimeout, err)
		}
		return val, err
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			var zero T
			return zero, res.Err
		}
		return res.Val.(T), nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// singleflightKey builds a collision-safe singleflight key from a structured (prefix,
// suffix) pair where either side may contain arbitrary bytes (including the would-be
// delimiter). The prefix's length is encoded as a decimal string followed by a NULL
// byte so different (prefix, suffix) pairs cannot map to the same string.
func singleflightKey(prefix, suffix string) string {
	return strconv.Itoa(len(prefix)) + "\x00" + prefix + suffix
}
