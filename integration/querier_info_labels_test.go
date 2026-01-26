// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQuerierInfoLabels(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{})

	// Start minio.
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Create client for pushing and querying.
	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	now := time.Now()

	// Push target_info metrics with data labels.
	// target_info is the standard info metric that contains identifying labels (job, instance)
	// and data labels (version, commit, etc.) that describe the target.
	targetInfoSeries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "target_info"},
				{Name: "job", Value: "prometheus"},
				{Name: "instance", Value: "localhost:9090"},
				{Name: "version", Value: "2.45.0"},
				{Name: "commit", Value: "abc123"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: now.UnixMilli()}},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "target_info"},
				{Name: "job", Value: "prometheus"},
				{Name: "instance", Value: "localhost:9091"},
				{Name: "version", Value: "2.44.0"},
				{Name: "commit", Value: "def456"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: now.UnixMilli()}},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "target_info"},
				{Name: "job", Value: "grafana"},
				{Name: "instance", Value: "localhost:3000"},
				{Name: "version", Value: "10.0.0"},
				{Name: "edition", Value: "oss"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: now.UnixMilli()}},
		},
	}

	// Also push a build_info metric to test metric_match parameter.
	buildInfoSeries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "build_info"},
				{Name: "job", Value: "mimir"},
				{Name: "instance", Value: "localhost:8080"},
				{Name: "branch", Value: "main"},
				{Name: "goversion", Value: "go1.21.0"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: now.UnixMilli()}},
		},
	}

	// Push all series.
	res, err := client.Push(targetInfoSeries)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	res, err = client.Push(buildInfoSeries)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	t.Run("basic info_labels query returns data labels from target_info", func(t *testing.T) {
		result, err := queryInfoLabels(client, querier.HTTPEndpoint(), nil)
		require.NoError(t, err)

		// Should return data labels from target_info (excludes identifying labels like job, instance, __name__)
		// Data labels are: version, commit, edition
		assert.Contains(t, result, "version")
		assert.Contains(t, result, "commit")
		assert.Contains(t, result, "edition")

		// Verify values are sorted and correct
		sort.Strings(result["version"])
		assert.Equal(t, []string{"10.0.0", "2.44.0", "2.45.0"}, result["version"])
	})

	t.Run("info_labels with metric_match returns data labels from specified info metric", func(t *testing.T) {
		result, err := queryInfoLabels(client, querier.HTTPEndpoint(), map[string]string{
			"metric_match": "build_info",
		})
		require.NoError(t, err)

		// Should return data labels from build_info
		assert.Contains(t, result, "branch")
		assert.Contains(t, result, "goversion")
		assert.Equal(t, []string{"main"}, result["branch"])
		assert.Equal(t, []string{"go1.21.0"}, result["goversion"])

		// Should NOT contain target_info labels
		assert.NotContains(t, result, "version")
		assert.NotContains(t, result, "commit")
	})

	t.Run("info_labels with match[] filters to base metrics", func(t *testing.T) {
		result, err := queryInfoLabels(client, querier.HTTPEndpoint(), map[string]string{
			"match[]": `{job="prometheus"}`,
		})
		require.NoError(t, err)

		// Should only return labels from target_info series where job=prometheus
		assert.Contains(t, result, "version")
		assert.Contains(t, result, "commit")

		// Should have prometheus versions only
		sort.Strings(result["version"])
		assert.Equal(t, []string{"2.44.0", "2.45.0"}, result["version"])

		// Should NOT have grafana-specific labels
		assert.NotContains(t, result, "edition")
	})

	t.Run("info_labels with limit truncates values", func(t *testing.T) {
		result, err := queryInfoLabels(client, querier.HTTPEndpoint(), map[string]string{
			"limit": "1",
		})
		require.NoError(t, err)

		// Each label should have at most 1 value due to limit
		for labelName, values := range result {
			assert.LessOrEqual(t, len(values), 1, "label %s should have at most 1 value", labelName)
		}
	})

	t.Run("info_labels with time range", func(t *testing.T) {
		// Query with a time range that includes our data
		start := now.Add(-1 * time.Hour)
		end := now.Add(1 * time.Hour)
		result, err := queryInfoLabels(client, querier.HTTPEndpoint(), map[string]string{
			"start": fmt.Sprintf("%d", start.Unix()),
			"end":   fmt.Sprintf("%d", end.Unix()),
		})
		require.NoError(t, err)

		// Should return data
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "version")
	})
}

// infoLabelsResponse represents the API response from /api/v1/info_labels.
type infoLabelsResponse struct {
	Status string              `json:"status"`
	Data   map[string][]string `json:"data"`
}

// queryInfoLabels queries the /api/v1/info_labels endpoint with optional parameters.
func queryInfoLabels(client *e2emimir.Client, querierAddress string, params map[string]string) (map[string][]string, error) {
	u, err := url.Parse(fmt.Sprintf("http://%s/prometheus/api/v1/info_labels", querierAddress))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	resp, body, err := client.DoGetBody(u.String())
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var result infoLabelsResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w, body: %s", err, string(body))
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("unexpected status: %s", result.Status)
	}

	return result.Data, nil
}
