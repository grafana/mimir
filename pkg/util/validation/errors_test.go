// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestNewMetadataMetricNameMissingError(t *testing.T) {
	err := newMetadataMetricNameMissingError()
	assert.Equal(t, "received a metric metadata with no metric name (err-mimir-metadata-missing-metric-name)", err.Error())
}

func TestNewMetadataMetricNameTooLongError(t *testing.T) {
	err := newMetadataMetricNameTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose metric name length exceeds the limit, metric name: 'test_metric' (err-mimir-metric-name-too-long). To adjust the related per-tenant limit, configure -validation.max-metadata-length, or contact your service administrator.", err.Error())
}

func TestNewMetadataHelpTooLongError(t *testing.T) {
	err := newMetadataHelpTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose help description length exceeds the limit, help: 'This is a test metric.' metric name: 'test_metric' (err-mimir-help-too-long). To adjust the related per-tenant limit, configure -validation.max-metadata-length, or contact your service administrator.", err.Error())
}

func TestNewMetadataUnitTooLongError(t *testing.T) {
	err := newMetadataUnitTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose unit name length exceeds the limit, unit: 'counter' metric name: 'test_metric' (err-mimir-unit-too-long). To adjust the related per-tenant limit, configure -validation.max-metadata-length, or contact your service administrator.", err.Error())
}

func TestNewMaxQueryLengthError(t *testing.T) {
	err := NewMaxQueryLengthError(time.Hour, time.Minute)
	assert.Equal(t, "the query time range exceeds the limit (query length: 1h0m0s, limit: 1m0s) (err-mimir-max-query-length). To adjust the related per-tenant limit, configure -store.max-query-length, or contact your service administrator.", err.Error())
}

func TestNewRequestRateLimitedError(t *testing.T) {
	err := NewRequestRateLimitedError(10, 5)
	assert.Equal(t, "the request has been rejected because the tenant exceeded the request rate limit, set to 10 requests/s across all distributors with a maximum allowed burst of 5 (err-mimir-tenant-max-request-rate). To adjust the related per-tenant limits, configure -distributor.request-rate-limit and -distributor.request-burst-size, or contact your service administrator.", err.Error())
}

func TestNewIngestionRateLimitedError(t *testing.T) {
	err := NewIngestionRateLimitedError(10, 5)
	assert.Equal(t, "the request has been rejected because the tenant exceeded the ingestion rate limit, set to 10 items/s with a maximum allowed burst of 5. This limit is applied on the total number of samples, exemplars and metadata received across all distributors (err-mimir-tenant-max-ingestion-rate). To adjust the related per-tenant limits, configure -distributor.ingestion-rate-limit and -distributor.ingestion-burst-size, or contact your service administrator.", err.Error())
}
