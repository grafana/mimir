// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestTenantInflightBytes(t *testing.T) {
	b := newTenantInflightBytes()
	require.Equal(t, int64(10), b.add("a", 10))
	require.Equal(t, int64(15), b.add("a", 5))
	require.Equal(t, int64(3), b.add("b", 3))
	require.Equal(t, int64(0), b.add("a", -15))
	require.Equal(t, int64(0), b.get("a"))
	require.NotContains(t, b.bytes, "a")
	require.Equal(t, int64(3), b.get("b"))
}

func TestDistributor_MaxInflightPushRequestsBytesPerTenant(t *testing.T) {
	const (
		compressedSize = 1000
		// The limit fits one in-flight request per tenant, but not two.
		limit = compressedSize + compressedSize/2
	)

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors: 1,
		limits:          &limits,
		configure: func(cfg *Config) {
			cfg.DefaultLimits.MaxInflightPushRequestsBytesPerTenant = limit
		},
	})
	d := ds[0]

	release := make(chan struct{})
	admitted := make(chan struct{}, 1)
	push := d.wrapPushWithMiddlewares(func(ctx context.Context, _ *Request) error {
		if userID, _ := user.ExtractOrgID(ctx); userID == "blocked" {
			admitted <- struct{}{}
			<-release
		}
		return nil
	})

	req := makeWriteRequest(0, 1, 0, false, false, "metric")
	doPush := func(userID string) error {
		ctx := user.InjectOrgID(context.Background(), userID)
		ctx, err := d.StartPushRequest(ctx, compressedSize)
		if err != nil {
			return err
		}
		defer d.FinishPushRequest(ctx)
		pushReq := NewParsedRequest(req, req.Size())
		defer pushReq.CleanUp()
		return push(ctx, pushReq)
	}

	// Hold one request in flight for the "blocked" tenant.
	firstDone := make(chan error, 1)
	go func() { firstDone <- doPush("blocked") }()
	<-admitted

	require.Equal(t, int64(compressedSize), d.tenantInflightBytes.get("blocked"))

	// A second request from the same tenant exceeds its limit, and is rejected with a 503 before it reaches the
	// reactive limiter: a reactive limiter that rejects every request would otherwise return its own error.
	d.reactiveLimiter = &mockReactiveLimiter{canAcquire: true, failOnAcquire: true}
	err := doPush("blocked")
	d.reactiveLimiter = nil
	require.ErrorIs(t, err, errMaxInflightRequestsBytesPerTenantReached)
	var distributorErr Error
	require.True(t, errors.As(err, &distributorErr))
	require.Equal(t, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT, distributorErr.Cause())
	require.Equal(t, 1.0, testutil.ToFloat64(d.discardedRequestsInflightBytesPerTenant.WithLabelValues("blocked")))
	require.Equal(t, 1.0, testutil.ToFloat64(d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequestsBytesPerTenant)))

	// Other tenants are unaffected.
	require.NoError(t, doPush("other"))

	close(release)
	require.NoError(t, <-firstDone)

	// All bytes are released once requests finish.
	require.Equal(t, int64(0), d.tenantInflightBytes.get("blocked"))
	require.Equal(t, int64(0), d.tenantInflightBytes.get("other"))
	require.NoError(t, doPush("blocked"))
}
