// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestNewMetadataMetricNameMissingError(t *testing.T) {
	err := newMetadataMetricNameMissingError()
	assert.Equal(t, "received a metric metadata with no metric name (err-mimir-metadata-missing-metric-name)", err.Error())
}

func TestNewMetadataMetricNameTooLongError(t *testing.T) {
	err := newMetadataMetricNameTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose metric name length exceeds the limit, metric name: 'test_metric' (err-mimir-metric-name-too-long). You can adjust the related per-tenant limit by configuring -validation.max-metadata-length, or by contacting your service administrator.", err.Error())
}

func TestNewMetadataHelpTooLongError(t *testing.T) {
	err := newMetadataHelpTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose help description length exceeds the limit, help: 'This is a test metric.' metric name: 'test_metric' (err-mimir-help-too-long). You can adjust the related per-tenant limit by configuring -validation.max-metadata-length, or by contacting your service administrator.", err.Error())
}

func TestNewMetadataUnitTooLongError(t *testing.T) {
	err := newMetadataUnitTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "test_metric", Unit: "counter", Help: "This is a test metric."})
	assert.Equal(t, "received a metric metadata whose unit name length exceeds the limit, unit: 'counter' metric name: 'test_metric' (err-mimir-unit-too-long). You can adjust the related per-tenant limit by configuring -validation.max-metadata-length, or by contacting your service administrator.", err.Error())
}
