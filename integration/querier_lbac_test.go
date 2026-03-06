// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	labelaccesspkg "github.com/grafana/mimir/pkg/labelaccess"
)

// TestLabelBasedAccessControl verifies end-to-end LBAC filtering:
//   - Without a policy header all series are returned.
//   - With a policy header only matching series are returned.
//   - Cardinality endpoints return HTTP 400 when a policy header is present.
func TestLabelBasedAccessControl(t *testing.T) {
	const tenantID = "lbac-tenant"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-auth.label-access-control-enabled": "true",
	})

	// Start query-scheduler first so that the query-frontend and querier can find it.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(ingester, distributor, querier))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait for ring convergence.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push two series that differ only in the value of the "access" label.
	now := time.Now()
	pushClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantID)
	require.NoError(t, err)

	seriesOpen, _, _ := generateFloatSeries("lbac_metric", now, prompb.Label{Name: "access", Value: "open"})
	res, err := pushClient.Push(seriesOpen)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	seriesRestricted, _, _ := generateFloatSeries("lbac_metric", now, prompb.Label{Name: "access", Value: "restricted"})
	res, err = pushClient.Push(seriesRestricted)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Without LBAC header: both series must be returned.
	queryClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenantID)
	require.NoError(t, err)
	result, err := queryClient.Query("lbac_metric", now)
	require.NoError(t, err)
	require.Len(t, result.(model.Vector), 2, "expected both series without LBAC policy")

	// With LBAC header restricting to {access="open"}: only the open series must be returned.
	lbacHeaderValue := fmt.Sprintf("%s:%s", tenantID, url.QueryEscape(`{access="open"}`))
	lbacClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenantID,
		e2emimir.WithAddHeader(labelaccesspkg.HTTPHeaderKey, lbacHeaderValue))
	require.NoError(t, err)

	result, err = lbacClient.Query("lbac_metric", now)
	require.NoError(t, err)
	vec := result.(model.Vector)
	require.Len(t, vec, 1, "expected exactly one series with LBAC policy")
	require.Equal(t, model.LabelValue("open"), vec[0].Metric["access"])

	// Cardinality endpoint must return HTTP 400 when a policy header is present.
	cardinalityURL := fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/label_values", queryFrontend.HTTPEndpoint())
	cardinalityReq, err := http.NewRequest(http.MethodPost, cardinalityURL, strings.NewReader("label_names[]=access"))
	require.NoError(t, err)
	cardinalityReq.Header.Set("X-Scope-OrgID", tenantID)
	cardinalityReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	cardinalityReq.Header.Set(labelaccesspkg.HTTPHeaderKey, lbacHeaderValue)

	cardResp, err := http.DefaultClient.Do(cardinalityReq)
	require.NoError(t, err)
	_ = cardResp.Body.Close()
	require.Equal(t, http.StatusBadRequest, cardResp.StatusCode, "cardinality endpoint must reject requests with LBAC policy header")
}
