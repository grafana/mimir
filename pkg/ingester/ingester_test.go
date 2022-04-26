// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

func mustNewActiveSeriesCustomTrackersConfigFromMap(t *testing.T, source map[string]string) activeseries.CustomTrackersConfig {
	m, err := activeseries.NewCustomTrackersConfig(source)
	require.NoError(t, err)
	return m
}

func TestIngester_Push(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := mimirpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricLabelSet := mimirpb.FromLabelAdaptersToMetric(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_discarded_samples_total",
		"cortex_ingester_active_series",
	}
	userID := "test"

	tests := map[string]struct {
		reqs                      []*mimirpb.WriteRequest
		expectedErr               error
		expectedIngested          model.Matrix
		expectedMetadataIngested  []*mimirpb.MetricMetadata
		expectedExemplarsIngested []mimirpb.TimeSeries
		expectedMetrics           string
		additionalMetrics         []string
		disableActiveSeries       bool
		maxExemplars              int
	}{
		"should succeed on valid series and metadata": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					[]*mimirpb.MetricMetadata{
						{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
					},
					mimirpb.API),
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					[]*mimirpb.MetricMetadata{
						{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGE},
					},
					mimirpb.API),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGE},
				{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed on valid series with exemplars": {
			maxExemplars: 2,
			reqs: []*mimirpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
					nil,
					mimirpb.API,
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API),
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					mimirpb.API),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
			`,
		},
		"should soft fail on sample out of order": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfOrderSample, model.Time(9), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-order",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on all samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  metricLabelAdapters,
								Samples: []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)}, {Value: 1, TimestampMs: 1575043969 - (86000 * 1000)}},
							},
						},
					},
				},
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(1575043969-(86400*1000)), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-bounds",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on some samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)},
									{Value: 1, TimestampMs: 1575043969 - (86000 * 1000)},
									{Value: 3, TimestampMs: 1575043969 + 1}},
							},
						},
					},
				},
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(1575043969-(86400*1000)), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}, {Value: 3, Timestamp: 1575043969 + 1}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-bounds",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrDuplicateSampleForTimestamp, model.Time(1575043969), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="new-value-for-timestamp",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on exemplar with unknown series": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				// This is not done here.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr:              httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestExemplarErr(errExemplarRef, model.Time(1000), mimirpb.FromLabelsToLabelAdapters(metricLabels), []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}), userID).Error()),
			expectedIngested:         nil,
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 0

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"should succeed with a request containing only metadata": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				{
					Metadata: []*mimirpb.MetricMetadata{
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
					},
				},
			},
			expectedErr:      nil,
			expectedIngested: nil,
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_active_appenders",
			},
			// NOTE cortex_ingester_memory_users is 0 here - the metric really counts tsdbs not users.
			// we may want to change that one day but for now make the test match the code.
			expectedMetrics: `
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 0
				# HELP cortex_ingester_tsdb_head_active_appenders Number of currently active TSDB appender transactions.
				# TYPE cortex_ingester_tsdb_head_active_appenders gauge
				cortex_ingester_tsdb_head_active_appenders 0
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			registry.MustRegister(validation.DiscardedSamples)
			validation.DiscardedSamples.Reset()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries
			limits := defaultLimitsTestConfig()
			limits.MaxGlobalExemplarsPerUser = testData.maxExemplars

			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// Push timeseries
			for idx, req := range testData.reqs {
				// Push metrics to the ingester. Override the default cleanup method of mimirpb.ReuseSlice with a no-op one.
				_, err := i.PushWithCleanup(ctx, req, func() {})

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					assert.NoError(t, err)
				} else {
					assert.Equal(t, testData.expectedErr, err)
				}
			}

			// Read back samples to see what has been really ingested
			s := &stream{ctx: ctx}
			err = i.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
			}, s)
			require.NoError(t, err)

			res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)
			if len(res) == 0 {
				res = nil
			}
			assert.Equal(t, testData.expectedIngested, res)

			// Read back samples to see what has been really ingested
			exemplarRes, err := i.QueryExemplars(ctx, &client.ExemplarQueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers: []*client.LabelMatchers{
					{Matchers: []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}}},
				},
			})

			require.NoError(t, err)
			require.NotNil(t, exemplarRes)
			assert.Equal(t, testData.expectedExemplarsIngested, exemplarRes.Timeseries)

			// Read back metadata to see what has been really ingested.
			mres, err := i.MetricsMetadata(ctx, &client.MetricsMetadataRequest{})

			require.NoError(t, err)
			require.NotNil(t, mres)

			// Order is never guaranteed.
			assert.ElementsMatch(t, testData.expectedMetadataIngested, mres.Metadata)

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.updateActiveSeries(time.Now())
			}

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)
		})
	}
}

func TestIngester_Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := mimirpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*mimirpb.WriteRequest{
			mimirpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				mimirpb.API,
			),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series for metrics check.
	i.updateActiveSeries(time.Now())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="test-1"} 2
		cortex_ingester_ingested_samples_total{user="test-2"} 2
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total{user="test-1"} 0
		cortex_ingester_ingested_samples_failures_total{user="test-2"} 0
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 2
		# HELP cortex_ingester_memory_series The current number of series in memory.
		# TYPE cortex_ingester_memory_series gauge
		cortex_ingester_memory_series 2
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 1
		cortex_ingester_active_series{user="test-2"} 1
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_Push_DecreaseInactiveSeries(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := mimirpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetricsIdleTimeout = 100 * time.Millisecond
	cfg.IngesterRing.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	currentTime := time.Now()
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*mimirpb.WriteRequest{
			mimirpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				mimirpb.API,
			),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series the after the idle timeout (in the future).
	// This will remove inactive series.
	currentTime = currentTime.Add(cfg.ActiveSeriesMetricsIdleTimeout + 1*time.Second)
	i.updateActiveSeries(currentTime)

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func BenchmarkIngesterPush(b *testing.B) {
	limits := defaultLimitsTestConfig()
	benchmarkIngesterPush(b, limits, false)
}

func benchmarkIngesterPush(b *testing.B, limits validation.Limits, errorsExpected bool) {
	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(b)
	cfg.IngesterRing.JoinAfter = 0

	ingester, err := prepareIngesterWithBlocksStorage(b, cfg, registry)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})

	// Push a single time series to set the TSDB min time.
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := mimirpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	startTime := util.TimeToMillis(time.Now())

	currTimeReq := mimirpb.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]mimirpb.Sample{{Value: 1, TimestampMs: startTime}},
		nil,
		nil,
		mimirpb.API,
	)
	_, err = ingester.Push(ctx, currTimeReq)
	require.NoError(b, err)

	const (
		series  = 10000
		samples = 10
	)

	allLabels, allSamples := benchmarkData(series)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		// Bump the timestamp on each of our test samples each time round the loop
		for j := 0; j < samples; j++ {
			for i := range allSamples {
				allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
			}
			_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(allLabels, allSamples, nil, nil, mimirpb.API))
			if !errorsExpected {
				require.NoError(b, err)
			}
		}
	}
}

func verifyErrorString(tb testing.TB, err error, expectedErr string) {
	if err == nil || !strings.Contains(err.Error(), expectedErr) {
		tb.Helper()
		tb.Fatalf("unexpected error. expected: %s actual: %v", expectedErr, err)
	}
}

func Benchmark_Ingester_PushOnError(b *testing.B) {
	var (
		ctx             = user.InjectOrgID(context.Background(), userID)
		sampleTimestamp = int64(100)
		metricName      = "test"
	)

	scenarios := map[string]struct {
		numSeriesPerRequest  int
		numConcurrentClients int
	}{
		"no concurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 1,
		},
		"low concurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 100,
		},
		"high concurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 1000,
		},
		"low number of series per request and very high concurrency": {
			numSeriesPerRequest:  100,
			numConcurrentClients: 2500,
		},
	}

	instanceLimits := map[string]*InstanceLimits{
		"no limits":  nil,
		"limits set": {MaxIngestionRate: 1e12, MaxInMemoryTenants: 1, MaxInMemorySeries: 500, MaxInflightPushRequests: 2500}, // these match max values from scenarios
	}

	tests := map[string]struct {
		// If this returns false, test is skipped.
		prepareConfig   func(limits *validation.Limits, instanceLimits *InstanceLimits) bool
		beforeBenchmark func(b *testing.B, ingester *Ingester, numSeriesPerRequest int)
		runBenchmark    func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample)
	}{
		"out of bound samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a single time series to set the TSDB min time.
				currTimeReq := mimirpb.ToWriteRequest(
					[]labels.Labels{{{Name: labels.MetricName, Value: metricName}}},
					[]mimirpb.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				expectedErr := storage.ErrOutOfBounds.Error()

				// Push out of bound samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"out of order samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// For each series, push a single sample with a timestamp greater than next pushes.
				for i := 0; i < numSeriesPerRequest; i++ {
					currTimeReq := mimirpb.ToWriteRequest(
						[]labels.Labels{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
						nil,
						nil,
						mimirpb.API)

					_, err := ingester.Push(ctx, currTimeReq)
					require.NoError(b, err)
				}
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				expectedErr := storage.ErrOutOfOrderSample.Error()

				// Push out of order samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"per-user series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxGlobalSeriesPerUser = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with a metric name different than the one used during the benchmark.
				currTimeReq := mimirpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, "another")},
					[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				// Push series with a different name than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-user series limit")
				}
			},
		},
		"per-metric series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxGlobalSeriesPerMetric = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with the same metric name but different labels than the one used during the benchmark.
				currTimeReq := mimirpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, metricName, "cardinality", "another")},
					[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-metric series limit")
				}
			},
		},
		"very low ingestion rate limit": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxIngestionRate = 0.00001 // very low
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send a lot of samples
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)

				ingester.ingestionRate.Tick()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "push rate limit reached")
				}
			},
		},
		"max number of tenants reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemoryTenants = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send some samples for one tenant (not the same that is used during the test)
				ctx := user.InjectOrgID(context.Background(), "different_tenant")
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "max tenants limit reached")
				}
			},
		},
		"max number of series reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemorySeries = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "max series limit reached")
				}
			},
		},
		"max inflight requests reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInflightPushRequests = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				ingester.inflightPushRequests.Inc()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []mimirpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "too many inflight push requests")
				}
			},
		},
	}

	for testName, testData := range tests {
		for scenarioName, scenario := range scenarios {
			for limitsName, limits := range instanceLimits {
				b.Run(fmt.Sprintf("failure: %s, scenario: %s, limits: %s", testName, scenarioName, limitsName), func(b *testing.B) {
					registry := prometheus.NewRegistry()

					instanceLimits := limits
					if instanceLimits != nil {
						// make a copy, to avoid changing value in the instanceLimits map.
						newLimits := &InstanceLimits{}
						*newLimits = *instanceLimits
						instanceLimits = newLimits
					}

					// Create a mocked ingester
					cfg := defaultIngesterTestConfig(b)
					cfg.IngesterRing.JoinAfter = 0

					limits := defaultLimitsTestConfig()
					if !testData.prepareConfig(&limits, instanceLimits) {
						b.SkipNow()
					}

					cfg.InstanceLimitsFn = func() *InstanceLimits {
						return instanceLimits
					}

					ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, "", registry)
					require.NoError(b, err)
					require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
					defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

					// Wait until the ingester is healthy
					test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
						return ingester.lifecycler.HealthyInstancesCount()
					})

					testData.beforeBenchmark(b, ingester, scenario.numSeriesPerRequest)

					// Prepare the request.
					metrics := make([]labels.Labels, 0, scenario.numSeriesPerRequest)
					samples := make([]mimirpb.Sample, 0, scenario.numSeriesPerRequest)
					for i := 0; i < scenario.numSeriesPerRequest; i++ {
						metrics = append(metrics, labels.Labels{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}})
						samples = append(samples, mimirpb.Sample{Value: float64(i), TimestampMs: sampleTimestamp})
					}

					// Run the benchmark.
					wg := sync.WaitGroup{}
					wg.Add(scenario.numConcurrentClients)
					start := make(chan struct{})

					b.ReportAllocs()
					b.ResetTimer()

					for c := 0; c < scenario.numConcurrentClients; c++ {
						go func() {
							defer wg.Done()
							<-start

							testData.runBenchmark(b, ingester, metrics, samples)
						}()
					}

					b.ResetTimer()
					close(start)
					wg.Wait()
				})
			}
		}
	}
}

func Test_Ingester_LabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_3"}, {Name: "status", Value: "500"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	t.Run("without matchers", func(t *testing.T) {
		expected := []string{"__name__", "status", "route"}

		// Get label names
		res, err := i.LabelNames(ctx, &client.LabelNamesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("with matchers", func(t *testing.T) {
		// test_2 and test_3 are selected in this test, they don't have the "route" label
		expected := []string{"__name__", "status"}

		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchNotEqual, "route", "get_user"),
		}

		req, err := client.ToLabelNamesRequest(0, model.Latest, matchers)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})
}

func Test_Ingester_LabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := map[string][]string{
		"__name__": {"test_1", "test_2"},
		"status":   {"200", "500"},
		"route":    {"get_user"},
		"unknown":  {},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Get label values
	for labelName, expectedValues := range expected {
		req := &client.LabelValuesRequest{LabelName: labelName, EndTimestampMs: math.MaxInt64}
		res, err := i.LabelValues(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expectedValues, res.LabelValues)
	}
}

func Test_Ingester_Query(t *testing.T) {
	series := []series{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatcher
		expected model.Matrix
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: model.Matrix{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
				&model.SampleStream{Metric: util.LabelsToMetric(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[2].lbls), Values: []model.SamplePair{{Value: 2, Timestamp: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
				&model.SampleStream{Metric: util.LabelsToMetric(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[2].lbls), Values: []model.SamplePair{{Value: 2, Timestamp: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: util.LabelsToMetric(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &client.QueryRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				Matchers:         testData.matchers,
			}

			s := stream{ctx: ctx}
			err = i.QueryStream(req, &s)
			require.NoError(t, err)

			res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res)
		})
	}
}

func TestIngester_LabelNamesCardinality(t *testing.T) {
	series := []series{
		{labels.Labels{{Name: labels.MetricName, Value: "metric_0"}, {Name: "status", Value: "500"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "metric_0"}, {Name: "status", Value: "200"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "metric_1"}, {Name: "env", Value: "prod"}}, 2, 200000},
		{labels.Labels{
			{Name: labels.MetricName, Value: "metric_1"},
			{Name: "env", Value: "prod"},
			{Name: "status", Value: "300"}}, 3, 200000},
	}

	tests := []struct {
		testName string
		matchers []*client.LabelMatcher
		expected []*client.LabelValues
	}{
		{testName: "expected all label with values",
			matchers: []*client.LabelMatcher{},
			expected: []*client.LabelValues{
				{LabelName: labels.MetricName, Values: []string{"metric_0", "metric_1"}},
				{LabelName: "status", Values: []string{"200", "300", "500"}},
				{LabelName: "env", Values: []string{"prod"}}},
		},
		{testName: "expected label values only from `metric_0`",
			matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "metric_0"}},
			expected: []*client.LabelValues{
				{LabelName: labels.MetricName, Values: []string{"metric_0"}},
				{LabelName: "status", Values: []string{"200", "500"}},
			},
		},
	}

	// Create ingester
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)

	ctx := pushSeriesToIngester(t, series, i)

	// Run tests
	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			req := &client.LabelNamesAndValuesRequest{
				Matchers: tc.matchers,
			}

			s := mockLabelNamesAndValuesServer{context: ctx}
			require.NoError(t, i.LabelNamesAndValues(req, &s))

			assert.ElementsMatch(t, extractItemsWithSortedValues(s.SentResponses), tc.expected)
		})
	}
}

func TestIngester_LabelValuesCardinality(t *testing.T) {
	series := []series{
		{
			lbls: labels.Labels{
				{Name: labels.MetricName, Value: "metric_0"},
				{Name: "status", Value: "500"},
			},
			value:     1.5,
			timestamp: 100000,
		},
		{
			lbls: labels.Labels{
				{Name: labels.MetricName, Value: "metric_0"},
				{Name: "status", Value: "200"},
			},
			value:     1.5,
			timestamp: 110030,
		},
		{
			lbls: labels.Labels{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "env", Value: "prod"},
			},
			value:     1.5,
			timestamp: 100060,
		},
		{
			lbls: labels.Labels{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "env", Value: "prod"},
				{Name: "status", Value: "300"},
			},
			value:     1.5,
			timestamp: 100090,
		},
	}
	tests := map[string]struct {
		labelNames    []string
		matchers      []*client.LabelMatcher
		expectedItems []*client.LabelValueSeriesCount
	}{
		"expected all label values cardinality": {
			labelNames: []string{labels.MetricName, "env", "status"},
			matchers:   []*client.LabelMatcher{},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName: "status",
					LabelValueSeries: map[string]uint64{
						"200": 1,
						"300": 1,
						"500": 1,
					},
				},
				{
					LabelName: labels.MetricName,
					LabelValueSeries: map[string]uint64{
						"metric_0": 2,
						"metric_1": 2,
					},
				},
				{
					LabelName: "env",
					LabelValueSeries: map[string]uint64{
						"prod": 2,
					},
				},
			},
		},
		"expected status values cardinality applying matchers": {
			labelNames: []string{"status"},
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: labels.MetricName, Value: "metric_1"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName:        "status",
					LabelValueSeries: map[string]uint64{"300": 1},
				},
			},
		},
		"empty response is returned when no matchers match the requested labels": {
			labelNames: []string{"status"},
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: "job", Value: "store-gateway"},
			},
			expectedItems: nil,
		},
	}

	// Create ingester
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)

	ctx := pushSeriesToIngester(t, series, i)
	// Run tests
	for tName, tc := range tests {
		t.Run(tName, func(t *testing.T) {
			req := &client.LabelValuesCardinalityRequest{
				LabelNames: tc.labelNames,
				Matchers:   tc.matchers,
			}

			s := &mockLabelValuesCardinalityServer{context: ctx}
			require.NoError(t, i.LabelValuesCardinality(req, s))

			if len(tc.expectedItems) == 0 {
				require.Len(t, s.SentResponses, 0)
				return
			}
			require.Len(t, s.SentResponses, 1)
			require.ElementsMatch(t, s.SentResponses[0].Items, tc.expectedItems)
		})
	}
}

func BenchmarkIngester_LabelValuesCardinality(b *testing.B) {
	var (
		userID              = "test"
		numSeries           = 10000
		numSamplesPerSeries = 60 * 6 // 6h on 1 sample per minute
		startTimestamp      = util.TimeToMillis(time.Now())
		step                = int64(60000) // 1 sample per minute
	)

	// Create ingester
	ing := createIngesterWithSeries(b, userID, numSeries, numSamplesPerSeries, startTimestamp, step)

	ctx := user.InjectOrgID(context.Background(), userID)
	s := &mockLabelValuesCardinalityServer{context: ctx}

	req := &client.LabelValuesCardinalityRequest{
		LabelNames: []string{labels.MetricName},
		Matchers: []*client.LabelMatcher{
			{Type: client.EQUAL, Name: "label_8", Value: "8"},
		},
	}
	b.Run("label values cardinality", func(b *testing.B) {
		// Run benchmarks
		for i := 0; i < b.N; i++ {
			err := ing.LabelValuesCardinality(req, s)
			require.NoError(b, err)
		}
	})
}

type series struct {
	lbls      labels.Labels
	value     float64
	timestamp int64
}

func pushSeriesToIngester(t testing.TB, series []series, i *Ingester) context.Context {
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	return ctx
}

func extractItemsWithSortedValues(responses []client.LabelNamesAndValuesResponse) []*client.LabelValues {
	var items []*client.LabelValues
	for _, res := range responses {
		items = append(items, res.Items...)
	}
	for _, it := range items {
		sort.Strings(it.Values)
	}
	return items
}

func TestIngester_Query_QuerySharding(t *testing.T) {
	const (
		numSeries = 1000
		numShards = 16
	)

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	// Push all series. We push half of the series, then we compact the TSDB head into a block (flush)
	// and finally we push the remaining series. This way we can both test querying back series both
	// from compacted blocks and head.
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := labels.Labels{
			{Name: labels.MetricName, Value: "foo"},
			{Name: "series_id", Value: strconv.Itoa(seriesID)},
		}

		req, _, _, _ := mockWriteRequest(t, lbls, float64(seriesID), int64(seriesID))
		_, err = i.Push(ctx, req)
		require.NoError(t, err)

		// Compact the TSDB head once half of the series have been pushed.
		if seriesID == numSeries/2 {
			i.Flush()
		}
	}

	// Query all series.
	var actualTimeseries model.Matrix

	for shardIndex := 0; shardIndex < numShards; shardIndex++ {
		req := &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"},
				{Type: client.EQUAL, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
					ShardIndex: uint64(shardIndex),
					ShardCount: uint64(numShards),
				}.LabelValue()},
			},
		}

		s := stream{ctx: ctx}
		err = i.QueryStream(req, &s)
		require.NoError(t, err)

		res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		actualTimeseries = append(actualTimeseries, res...)
	}

	// We expect that all series have been returned.
	assert.Len(t, actualTimeseries, numSeries)

	actualSeriesIDs := map[int]struct{}{}

	for _, series := range actualTimeseries {
		seriesID, err := strconv.Atoi(string(series.Metric[model.LabelName("series_id")]))
		require.NoError(t, err)

		// We expect no duplicated series in the result.
		_, exists := actualSeriesIDs[seriesID]
		assert.False(t, exists)
		actualSeriesIDs[seriesID] = struct{}{}

		// We expect 1 sample with the same timestamp and value we've written.
		require.Len(t, series.Values, 1)
		assert.Equal(t, int64(seriesID), int64(series.Values[0].Timestamp))
		assert.Equal(t, float64(seriesID), float64(series.Values[0].Value))
	}
}

func TestIngester_Query_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.QueryRequest{}

	s := stream{ctx: ctx}
	err = i.QueryStream(req, &s)
	require.NoError(t, err)

	assert.Empty(t, s.responses)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_LabelValues_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelValuesRequest{}

	res, err := i.LabelValues(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelValuesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_LabelNames_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelNamesRequest{EndTimestampMs: math.MaxInt64}

	res, err := i.LabelNames(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelNamesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_Push_ShouldNotCreateTSDBIfNotInActiveState(t *testing.T) {
	// Configure the lifecycler to not immediately join the ring, to make sure
	// the ingester will NOT be in the ACTIVE state when we'll push samples.
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 10 * time.Second

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, 0)

	res, err := i.Push(ctx, req)
	assert.EqualError(t, err, wrapWithUser(fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), userID).Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_getOrCreateTSDB_ShouldNotAllowToCreateTSDBIfIngesterStateIsNotActive(t *testing.T) {
	tests := map[string]struct {
		state       ring.InstanceState
		expectedErr error
	}{
		"not allow to create TSDB if in PENDING state": {
			state:       ring.PENDING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.PENDING),
		},
		"not allow to create TSDB if in JOINING state": {
			state:       ring.JOINING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.JOINING),
		},
		"not allow to create TSDB if in LEAVING state": {
			state:       ring.LEAVING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.LEAVING),
		},
		"allow to create TSDB if in ACTIVE state": {
			state:       ring.ACTIVE,
			expectedErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 60 * time.Second

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Switch ingester state to the expected one in the test
			if i.lifecycler.GetState() != testData.state {
				var stateChain []ring.InstanceState

				if testData.state == ring.LEAVING {
					stateChain = []ring.InstanceState{ring.ACTIVE, ring.LEAVING}
				} else {
					stateChain = []ring.InstanceState{testData.state}
				}

				for _, s := range stateChain {
					err = i.lifecycler.ChangeState(context.Background(), s)
					require.NoError(t, err)
				}
			}

			db, err := i.getOrCreateTSDB("test", false)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr != nil {
				assert.Nil(t, db)
			} else {
				assert.NotNil(t, db)
			}
		})
	}
}

func Test_Ingester_MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatchers
		expected []*mimirpb.Metric
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*mimirpb.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should filter metrics by time range to return nothing when queried for older time ranges": {
			from: 100,
			to:   1000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*mimirpb.Metric{},
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
			}

			res, err := i.MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)
		})
	}
}

func Test_Ingester_MetricsForLabelMatchers_Deduplication(t *testing.T) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeries(t, userID, numSeries, 1, now, 1)
	ctx := user.InjectOrgID(context.Background(), "test")

	req := &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: now,
		EndTimestampMs:   now,
		// Overlapping matchers to make sure series are correctly deduplicated.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}},
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*0"},
			}},
		},
	}

	res, err := i.MetricsForLabelMatchers(ctx, req)
	require.NoError(t, err)
	require.Len(t, res.GetMetric(), numSeries)
}

func Benchmark_Ingester_MetricsForLabelMatchers(b *testing.B) {
	var (
		userID              = "test"
		numSeries           = 10000
		numSamplesPerSeries = 60 * 6 // 6h on 1 sample per minute
		startTimestamp      = util.TimeToMillis(time.Now())
		step                = int64(60000) // 1 sample per minute
	)

	i := createIngesterWithSeries(b, userID, numSeries, numSamplesPerSeries, startTimestamp, step)
	ctx := user.InjectOrgID(context.Background(), "test")

	// Flush the ingester to ensure blocks have been compacted, so we'll test
	// fetching labels from blocks.
	i.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		req := &client.MetricsForLabelMatchersRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}}},
		}

		res, err := i.MetricsForLabelMatchers(ctx, req)
		require.NoError(b, err)
		require.Len(b, res.GetMetric(), numSeries)
	}
}

// createIngesterWithSeries creates an ingester and push numSeries with numSamplesPerSeries each.
func createIngesterWithSeries(t testing.TB, userID string, numSeries, numSamplesPerSeries int, startTimestamp, step int64) *Ingester {
	const maxBatchSize = 1000

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push fixtures.
	ctx := user.InjectOrgID(context.Background(), userID)

	for ts := startTimestamp; ts < startTimestamp+(step*int64(numSamplesPerSeries)); ts += step {
		for o := 0; o < numSeries; o += maxBatchSize {
			batchSize := util_math.Min(maxBatchSize, numSeries-o)

			// Generate metrics and samples (1 for each series).
			metrics := make([]labels.Labels, 0, batchSize)
			samples := make([]mimirpb.Sample, 0, batchSize)

			for s := 0; s < batchSize; s++ {
				metrics = append(metrics, labels.Labels{
					{Name: labels.MetricName, Value: fmt.Sprintf("test_%d", o+s)},
				})

				samples = append(samples, mimirpb.Sample{
					TimestampMs: ts,
					Value:       1,
				})
			}

			// Send metrics to the ingester.
			req := mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	return i
}

func TestIngester_QueryStream(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	// Change stream type in runtime.
	var streamType QueryStreamType
	cfg.StreamTypeFn = func() QueryStreamType {
		return streamType
	}

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	// Push all series. We push half of the series, then we compact the TSDB head into a block (flush)
	// and finally we push the remaining series. This way we can both test querying back series both
	// from compacted blocks and head.
	const numSeries = 1000

	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := labels.Labels{
			{Name: labels.MetricName, Value: "foo"},
			{Name: "series_id", Value: strconv.Itoa(seriesID)},
		}

		req, _, _, _ := mockWriteRequest(t, lbls, float64(seriesID), int64(seriesID))
		_, err = i.Push(ctx, req)
		require.NoError(t, err)

		// Compact the TSDB head once half of the series have been pushed.
		if seriesID == numSeries/2 {
			i.Flush()
		}
	}

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	tests := map[string]struct {
		streamType         QueryStreamType
		numShards          int
		expectedStreamType QueryStreamType
	}{
		"should query chunks by default": {
			streamType:         QueryStreamDefault,
			expectedStreamType: QueryStreamChunks,
		},
		"should query samples when configured with QueryStreamSamples": {
			streamType:         QueryStreamSamples,
			expectedStreamType: QueryStreamSamples,
		},
		"should query chunks when configured with QueryStreamChunks": {
			streamType:         QueryStreamChunks,
			expectedStreamType: QueryStreamChunks,
		},
		"should support sharding when query samples": {
			streamType:         QueryStreamSamples,
			numShards:          16,
			expectedStreamType: QueryStreamSamples,
		},
		"should support sharding when query chunks": {
			streamType:         QueryStreamChunks,
			numShards:          16,
			expectedStreamType: QueryStreamChunks,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Configure the stream type.
			streamType = testData.streamType

			// Query all series.
			var actualTimeseries []mimirpb.TimeSeries
			var actualChunkseries []client.TimeSeriesChunk

			runQueryAndSaveResponse := func(req *client.QueryRequest) (receivedSeries int, err error) {
				s, err := c.QueryStream(ctx, req)
				if err != nil {
					return 0, err
				}

				for {
					resp, err := s.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return receivedSeries, err
					}

					actualTimeseries = append(actualTimeseries, resp.Timeseries...)
					actualChunkseries = append(actualChunkseries, resp.Chunkseries...)

					receivedSeries += len(resp.Timeseries)
					receivedSeries += len(resp.Chunkseries)
				}

				return receivedSeries, nil
			}

			if testData.numShards > 0 {
				for shardIndex := 0; shardIndex < testData.numShards; shardIndex++ {
					receivedSeries, err := runQueryAndSaveResponse(&client.QueryRequest{
						StartTimestampMs: math.MinInt64,
						EndTimestampMs:   math.MaxInt64,
						Matchers: []*client.LabelMatcher{
							{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"},
							{Type: client.EQUAL, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
								ShardIndex: uint64(shardIndex),
								ShardCount: uint64(testData.numShards),
							}.LabelValue()},
						},
					})

					require.NoError(t, err)
					assert.Greater(t, receivedSeries, 0)
				}
			} else {
				receivedSeries, err := runQueryAndSaveResponse(&client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"}},
				})

				require.NoError(t, err)
				assert.Greater(t, receivedSeries, 0)
			}

			// Ensure we received the expected data types in response.
			if testData.expectedStreamType == QueryStreamSamples {
				assert.Len(t, actualTimeseries, numSeries)
				assert.Empty(t, actualChunkseries)
			} else {
				assert.Len(t, actualChunkseries, numSeries)
				assert.Empty(t, actualTimeseries)
			}

			// We expect that all series have been returned.
			actualSeriesIDs := map[int]struct{}{}

			for _, series := range actualTimeseries {
				seriesID, err := strconv.Atoi(mimirpb.FromLabelAdaptersToLabels(series.Labels).Get("series_id"))
				require.NoError(t, err)

				// We expect no duplicated series in the result.
				_, exists := actualSeriesIDs[seriesID]
				assert.False(t, exists)
				actualSeriesIDs[seriesID] = struct{}{}

				// We expect 1 sample with the same timestamp and value we've written.
				require.Len(t, series.Samples, 1)
				assert.Equal(t, int64(seriesID), series.Samples[0].TimestampMs)
				assert.Equal(t, float64(seriesID), series.Samples[0].Value)
			}

			for _, series := range actualChunkseries {
				seriesID, err := strconv.Atoi(mimirpb.FromLabelAdaptersToLabels(series.Labels).Get("series_id"))
				require.NoError(t, err)

				// We expect no duplicated series in the result.
				_, exists := actualSeriesIDs[seriesID]
				assert.False(t, exists)
				actualSeriesIDs[seriesID] = struct{}{}

				// We expect 1 chunk.
				require.Len(t, series.Chunks, 1)

				data, err := chunkenc.FromData(chunkenc.EncXOR, series.Chunks[0].Data)
				require.NoError(t, err)

				// We expect 1 sample with the same timestamp and value we've written.
				it := data.Iterator(nil)

				require.True(t, it.Next())
				actualTs, actualValue := it.At()
				assert.Equal(t, int64(seriesID), actualTs)
				assert.Equal(t, float64(seriesID), actualValue)

				assert.False(t, it.Next())
				assert.NoError(t, it.Err())
			}
		})
	}
}

func TestIngester_QueryStreamManySamples(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.StreamChunksWhenUsingBlocks = false

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 100000
	samples := make([]mimirpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 10k samples encode to around 140 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:10000]))
	require.NoError(t, err)

	// 100k samples encode to around 1.4 MiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 50k samples encode to around 716 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:50000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Timeseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Timeseries)

		for _, ts := range resp.Timeseries {
			totalSamples += len(ts.Samples)
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (10k + 50k in first, 100k in second)
	// or 3 messages (small series first, 100k second, small series last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 10000+50000+samplesCount, totalSamples)
}

func TestIngester_QueryStreamManySamplesChunks(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.StreamChunksWhenUsingBlocks = true

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000000
	samples := make([]mimirpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 100k samples in chunks use about 154 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:100000]))
	require.NoError(t, err)

	// 1M samples in chunks use about 1.51 MiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 500k samples in chunks need 775 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:500000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Chunkseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Chunkseries)

		for _, ts := range resp.Chunkseries {
			for _, c := range ts.Chunks {
				ch, err := chunk.NewForEncoding(chunk.Encoding(c.Encoding))
				require.NoError(t, err)
				require.NoError(t, ch.UnmarshalFromBuf(c.Data))

				totalSamples += ch.Len()
			}
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (100k + 500k in first, 1M in second)
	// or 3 messages (100k or 500k first, 1M second, and 500k or 100k last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+samplesCount, totalSamples)
}

func writeRequestSingleSeries(lbls labels.Labels, samples []mimirpb.Sample) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{
		Source: mimirpb.API,
	}

	ts := mimirpb.TimeSeries{}
	ts.Labels = mimirpb.FromLabelsToLabelAdapters(lbls)
	ts.Samples = samples
	req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &ts})

	return req
}

type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockQueryStreamServer) Send(response *client.QueryStreamResponse) error {
	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

func BenchmarkIngester_QueryStream(b *testing.B) {
	const (
		numSeries       = 25000 // Number of series to push.
		numHeadSamples  = 240   // Number of samples in the TSDB Head (2h of samples at 30s scrape interval).
		numBlockSamples = 240   // Number of samples in compacted blocks (2h of samples at 30s scrape interval).
		numShards       = 16    // Number of shards to query when query sharding is enabled.
	)

	cfg := defaultIngesterTestConfig(b)
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0

	// Change stream type in runtime.
	var streamType QueryStreamType
	cfg.StreamTypeFn = func() QueryStreamType {
		return streamType
	}

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, "", nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	b.Cleanup(func() {
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(b, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series to a compacted block.
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := make([]mimirpb.Sample, 0, numBlockSamples)

	for i := 0; i < numBlockSamples; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	for s := 0; s < numSeries; s++ {
		_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: strconv.Itoa(s)}}, samples))
		require.NoError(b, err)
	}

	i.Flush()

	// Push series to TSDB head.
	samples = samples[:0]
	for i := numBlockSamples; i < numBlockSamples+numHeadSamples; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	for s := 0; s < numSeries; s++ {
		_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: strconv.Itoa(s)}}, samples))
		require.NoError(b, err)
	}

	// Benchmark different ranges.
	ranges := []struct {
		name       string
		start, end int64
	}{
		{
			name:  "full data",
			start: math.MinInt64,
			end:   math.MaxInt64,
		},
		{
			name:  "partial block",
			start: 1,
			end:   numBlockSamples / 3,
		},
		{
			name:  "partial head",
			start: numBlockSamples + 5,
			end:   numBlockSamples + numHeadSamples - 5,
		},
		{
			name:  "head + partial block",
			start: numBlockSamples / 3,
			end:   numBlockSamples + numHeadSamples,
		},
	}

	b.Run("query samples", func(b *testing.B) {
		streamType = QueryStreamSamples

		for _, timeRange := range ranges {
			for _, queryShardingEnabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("time range=%v, query sharding=%v", timeRange.name, queryShardingEnabled), func(b *testing.B) {
					benchmarkIngesterQueryStream(ctx, b, i, timeRange.start, timeRange.end, queryShardingEnabled, numShards)
				})
			}
		}
	})

	b.Run("query chunks", func(b *testing.B) {
		streamType = QueryStreamChunks

		for _, timeRange := range ranges {
			for _, queryShardingEnabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("time range=%v, query sharding=%v", timeRange.name, queryShardingEnabled), func(b *testing.B) {
					benchmarkIngesterQueryStream(ctx, b, i, timeRange.start, timeRange.end, queryShardingEnabled, numShards)
				})
			}
		}
	})
}

func requireActiveIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) *Ingester {
	ingester := getStartedIngesterWithBlocksStorage(t, ingesterCfg, registerer)
	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})
	return ingester
}

func getStartedIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) *Ingester {
	ingester, err := prepareIngesterWithBlocksStorage(t, ingesterCfg, registerer)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})
	return ingester
}

func benchmarkIngesterQueryStream(ctx context.Context, b *testing.B, i *Ingester, start, end int64, queryShardingEnabled bool, numShards int) {
	mockStream := &mockQueryStreamServer{ctx: ctx}

	metricMatcher := &client.LabelMatcher{
		Type:  client.EQUAL,
		Name:  model.MetricNameLabel,
		Value: "foo",
	}

	for ix := 0; ix < b.N; ix++ {
		if queryShardingEnabled {
			// Query each shard.
			for idx := 0; idx < numShards; idx++ {
				req := &client.QueryRequest{
					StartTimestampMs: start,
					EndTimestampMs:   end,
					Matchers: []*client.LabelMatcher{metricMatcher, {
						Type:  client.EQUAL,
						Name:  sharding.ShardLabel,
						Value: sharding.ShardSelector{ShardIndex: uint64(idx), ShardCount: uint64(numShards)}.LabelValue(),
					}},
				}

				err := i.QueryStream(req, mockStream)
				require.NoError(b, err)
			}
		} else {
			req := &client.QueryRequest{
				StartTimestampMs: start,
				EndTimestampMs:   end,
				Matchers:         []*client.LabelMatcher{metricMatcher},
			}

			err := i.QueryStream(req, mockStream)
			require.NoError(b, err)
		}
	}
}

func mockWriteRequest(t testing.TB, lbls labels.Labels, value float64, timestampMs int64) (*mimirpb.WriteRequest, *client.QueryResponse, *client.QueryStreamResponse, *client.QueryStreamResponse) {
	samples := []mimirpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := mimirpb.ToWriteRequest([]labels.Labels{lbls}, samples, nil, nil, mimirpb.API)

	// Generate the expected response
	expectedQueryRes := &client.QueryResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	expectedQueryStreamResSamples := &client.QueryStreamResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	require.NoError(t, err)
	app.Append(timestampMs, value)
	chk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: mimirpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         int32(chunk.PrometheusXorChunk),
						Data:             chk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryRes, expectedQueryStreamResSamples, expectedQueryStreamResChunks
}

func prepareIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, error) {
	return prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, defaultLimitsTestConfig(), "", registerer)
}

func prepareIngesterWithBlocksStorageAndLimits(t testing.TB, ingesterCfg Config, limits validation.Limits, dataDir string, registerer prometheus.Registerer) (*Ingester, error) {
	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, err
	}
	return prepareIngesterWithBlockStorageAndOverrides(t, ingesterCfg, overrides, dataDir, registerer)
}

func prepareIngesterWithBlockStorageAndOverrides(t testing.TB, ingesterCfg Config, overrides *validation.Overrides, dataDir string, registerer prometheus.Registerer) (*Ingester, error) {
	// Create a data dir if none has been provided.
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	bucketDir := t.TempDir()
	clientCfg := defaultClientTestConfig()

	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dataDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = bucketDir

	ingester, err := New(ingesterCfg, clientCfg, overrides, registerer, log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func TestIngester_OpenExistingTSDBOnStartup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		concurrency int
		setup       func(*testing.T, string)
		check       func(*testing.T, *Ingester)
		expectedErr string
	}{
		"should not load TSDB if the user directory is empty": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user0"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Nil(t, i.getTSDB("user0"))
			},
		},
		"should not load any TSDB if the root directory is empty": {
			concurrency: 10,
			setup:       func(t *testing.T, dir string) {},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.tsdbs))
			},
		},
		"should not load any TSDB is the root directory is missing": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(dir))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.tsdbs))
			},
		},
		"should load TSDB for any non-empty user directory": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user2"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 2, len(i.tsdbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
			},
		},
		"should load all TSDBs on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.tsdbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.NotNil(t, i.getTSDB("user2"))
				require.NotNil(t, i.getTSDB("user3"))
				require.NotNil(t, i.getTSDB("user4"))
			},
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency > number of TSDBs": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "chunks_head", ""), 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000002"), nil, 0700))

				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.tsdbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
			},
			expectedErr: "unable to open TSDB for user user0",
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))

				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "chunks_head", ""), 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000002"), nil, 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.tsdbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
				require.Nil(t, i.getTSDB("user3"))
				require.Nil(t, i.getTSDB("user4"))
			},
			expectedErr: "unable to open TSDB for user user2",
		},
	}

	for name, test := range tests {
		testName := name
		testData := test
		t.Run(testName, func(t *testing.T) {
			clientCfg := defaultClientTestConfig()
			limits := defaultLimitsTestConfig()

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Create a temporary directory for TSDB
			tempDir := t.TempDir()

			ingesterCfg := defaultIngesterTestConfig(t)
			ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
			ingesterCfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup = testData.concurrency
			ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
			ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"

			// setup the tsdbs dir
			testData.setup(t, tempDir)

			ingester, err := New(ingesterCfg, clientCfg, overrides, nil, log.NewNopLogger())
			require.NoError(t, err)

			startErr := services.StartAndAwaitRunning(context.Background(), ingester)
			if testData.expectedErr == "" {
				require.NoError(t, startErr)
			} else {
				require.Error(t, startErr)
				assert.Contains(t, startErr.Error(), testData.expectedErr)
			}

			defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
			testData.check(t, ingester)
		})
	}
}

func TestIngester_shipBlocks(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Create the TSDB for 3 users and then replace the shipper with the mocked one
	mocks := []*shipperMock{}
	for _, userID := range []string{"user-1", "user-2", "user-3"} {
		userDB, err := i.getOrCreateTSDB(userID, false)
		require.NoError(t, err)
		require.NotNil(t, userDB)

		m := &shipperMock{}
		m.On("Sync", mock.Anything).Return(0, nil)
		mocks = append(mocks, m)

		userDB.shipper = m
	}

	// Ship blocks and assert on the mocked shipper
	i.shipBlocks(context.Background(), nil)

	for _, m := range mocks {
		m.AssertNumberOfCalls(t, "Sync", 1)
	}
}

func TestIngester_dontShipBlocksWhenTenantDeletionMarkerIsPresent(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	i.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjects := len(bucket.Objects())
	require.NotZero(t, numObjects)

	require.NoError(t, mimir_tsdb.WriteTenantDeletionMark(context.Background(), bucket, userID, nil, mimir_tsdb.NewTenantDeletionMark(time.Now())))
	numObjects++ // For deletion marker

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	db.lastDeletionMarkCheck.Store(0)

	// After writing tenant deletion mark,
	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjectsAfterMarkingTenantForDeletion := len(bucket.Objects())
	require.Equal(t, numObjects, numObjectsAfterMarkingTenantForDeletion)
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_seriesCountIsCorrectAfterClosingTSDBForDeletedTenant(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	// Write tenant deletion mark.
	require.NoError(t, mimir_tsdb.WriteTenantDeletionMark(context.Background(), bucket, userID, nil, mimir_tsdb.NewTenantDeletionMark(time.Now())))

	i.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())

	// We call shipBlocks to check for deletion marker (it happens inside this method).
	i.shipBlocks(context.Background(), nil)

	// Verify that tenant deletion mark was found.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.deletionMarkFound.Load())

	// If we try to close TSDB now, it should succeed, even though TSDB is not idle and empty.
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Closing should decrease series count.
	require.Equal(t, int64(0), i.seriesCount.Load())
}

func TestIngester_closeAndDeleteUserTSDBIfIdle_shouldNotCloseTSDBIfShippingIsInProgress(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// We want it to be idle immediately (setting to 1ns because 0 means disabled).
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = time.Nanosecond

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Mock the shipper to slow down Sync() execution.
	s := mockUserShipper(t, i)
	s.On("Sync", mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(3 * time.Second)
	}).Return(0, nil)

	// Mock the shipper meta (no blocks).
	db := i.getTSDB(userID)
	require.NoError(t, shipper.WriteMetaFile(log.NewNopLogger(), db.db.Dir(), &shipper.Meta{
		Version: shipper.MetaVersion1,
	}))

	// Run blocks shipping in a separate go routine.
	go i.shipBlocks(ctx, nil)

	// Wait until shipping starts.
	test.Poll(t, 1*time.Second, activeShipping, func() interface{} {
		db.stateMtx.RLock()
		defer db.stateMtx.RUnlock()
		return db.state
	})

	assert.Equal(t, tsdbNotActive, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_closingAndOpeningTsdbConcurrently(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	_, err = i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)

	iterations := 5000
	chanErr := make(chan error, 1)
	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				_, err = i.getOrCreateTSDB(userID, false)
				if err != nil {
					chanErr <- err
				}
			}
		}
	}()

	for k := 0; k < iterations; k++ {
		i.closeAndDeleteUserTSDBIfIdle(userID)
	}

	select {
	case err := <-chanErr:
		assert.Fail(t, err.Error())
		quit <- true
	default:
		quit <- true
	}
}

func TestIngester_idleCloseEmptyTSDB(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	db, err := i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Run compaction and shipping.
	i.compactBlocks(context.Background(), true, nil)
	i.shipBlocks(context.Background(), nil)

	// Make sure we can close completely empty TSDB without problems.
	require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Verify that it was closed.
	db = i.getTSDB(userID)
	require.Nil(t, db)

	// And we can recreate it again, if needed.
	db, err = i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)
}

type shipperMock struct {
	mock.Mock
}

// Sync mocks Shipper.Sync()
func (m *shipperMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func TestIngester_invalidSamplesDontChangeLastUpdateTime(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sampleTimestamp := int64(model.Now())

	{
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	lastUpdate := db.lastUpdate.Load()

	// Wait until 1 second passes.
	test.Poll(t, 1*time.Second, time.Now().Unix()+1, func() interface{} {
		return time.Now().Unix()
	})

	// Push another sample to the same metric and timestamp, with different value. We expect to get error.
	{
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 1, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.Error(t, err)
	}

	// Make sure last update hasn't changed.
	require.Equal(t, lastUpdate, db.lastUpdate.Load())
}

func TestIngester_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, reg *prometheus.Registry)
	}{
		"ingesterShutdown": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"shutdownHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/shutdown", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandlerWithListOfTenants": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				users := url.Values{}
				users.Add(tenantParam, "unknown-user")
				users.Add(tenantParam, "another-unknown-user")

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true&"+users.Encode(), nil))

				// Still nothing shipped or compacted.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))
				verifyCompactedHead(t, i, false)

				users = url.Values{}
				users.Add(tenantParam, "different-user")
				users.Add(tenantParam, userID) // Our user
				users.Add(tenantParam, "yet-another-user")

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true&"+users.Encode(), nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushMultipleBlocksWithDataSpanning3Days": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				// Pushing 5 samples, spanning over 3 days.
				// First block
				pushSingleSampleAtTime(t, i, 23*time.Hour.Milliseconds())
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()-1)

				// Second block
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()+1)
				pushSingleSampleAtTime(t, i, 25*time.Hour.Milliseconds())

				// Third block, far in the future.
				pushSingleSampleAtTime(t, i, 50*time.Hour.Milliseconds())

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 3
				`), "cortex_ingester_shipper_uploads_total"))

				userDB := i.getTSDB(userID)
				require.NotNil(t, userDB)

				blocks := userDB.Blocks()
				require.Equal(t, 3, len(blocks))
				require.Equal(t, 23*time.Hour.Milliseconds(), blocks[0].Meta().MinTime)
				require.Equal(t, 24*time.Hour.Milliseconds(), blocks[0].Meta().MaxTime) // Block maxt is exclusive.

				require.Equal(t, 24*time.Hour.Milliseconds()+1, blocks[1].Meta().MinTime)
				require.Equal(t, 26*time.Hour.Milliseconds(), blocks[1].Meta().MaxTime)

				require.Equal(t, 50*time.Hour.Milliseconds()+1, blocks[2].Meta().MaxTime) // Block maxt is exclusive.
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 0
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute // Long enough to not be reached during the test.

			if tc.setupIngester != nil {
				tc.setupIngester(&cfg)
			}

			// Create ingester
			reg := prometheus.NewPedanticRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's healthy
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// mock user's shipper
			tc.action(t, i, reg)
		})
	}
}

func TestIngester_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	reg = prometheus.NewPedanticRegistry()
	i, err = NewForFlusher(i.cfg, i.limits, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
}

func mockUserShipper(t *testing.T, i *Ingester) *shipperMock {
	m := &shipperMock{}
	userDB, err := i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)
	require.NotNil(t, userDB)

	userDB.shipper = m
	return m
}

func Test_Ingester_UserStats(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.tsdbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	assert.Equal(t, uint64(3), res.NumSeries)
}

func Test_Ingester_AllUserStats(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.tsdbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.AllUserStats(context.Background(), &client.UserStatsRequest{})
	require.NoError(t, err)

	expect := []*client.UserIDStatsResponse{
		{
			UserId: "user-1",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.2,
				NumSeries:         3,
				ApiIngestionRate:  0.2,
				RuleIngestionRate: 0,
			},
		},
		{
			UserId: "user-2",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.13333333333333333,
				NumSeries:         2,
				ApiIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
			},
		},
	}
	assert.ElementsMatch(t, expect, res.Stats)
}

func TestIngesterCompactIdleBlock(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))

	// Pushing another sample still works.
	pushSingleSampleWithMetadata(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))
}

func TestIngesterCompactAndCloseIdleTSDB(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Second // Required to enable shipping.
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 100 * time.Millisecond
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = 100 * time.Millisecond

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(time.Now())

	require.Equal(t, int64(1), i.seriesCount.Load())

	metricsToCheck := []string{"cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users", "cortex_ingester_active_series",
		"cortex_ingester_memory_metadata", "cortex_ingester_memory_metadata_created_total", "cortex_ingester_memory_metadata_removed_total"}

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))

	// Wait until TSDB has been closed and removed.
	test.Poll(t, 20*time.Second, 0, func() interface{} {
		i.tsdbsMtx.Lock()
		defer i.tsdbsMtx.Unlock()
		return len(i.tsdbs)
	})

	require.Greater(t, testutil.ToFloat64(i.metrics.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))), float64(0))
	i.updateActiveSeries(time.Now())
	require.Equal(t, int64(0), i.seriesCount.Load()) // Flushing removed all series from memory.

	// Verify that user has disappeared from metrics.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 0

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
    `), metricsToCheck...))

	// Pushing another sample will recreate TSDB.
	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(time.Now())

	// User is back.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))
}

func verifyCompactedHead(t *testing.T, i *Ingester, expected bool) {
	db := i.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()
	require.Equal(t, expected, h.NumSeries() == 0)
}

func pushSingleSampleWithMetadata(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	req.Metadata = append(req.Metadata, &mimirpb.MetricMetadata{MetricFamilyName: "test", Help: "a help for metric", Unit: "", Type: mimirpb.COUNTER})
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func pushSingleSampleAtTime(t *testing.T, i *Ingester, ts int64) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, ts)
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func TestHeadCompactionOnStartup(t *testing.T) {
	// Create a temporary directory for TSDB
	tempDir := t.TempDir()

	// Build TSDB for user, with data covering 24 hours.
	{
		// Number of full chunks, 12 chunks for 24hrs.
		numFullChunks := 12
		chunkRange := 2 * time.Hour.Milliseconds()

		userDir := filepath.Join(tempDir, userID)
		require.NoError(t, os.Mkdir(userDir, 0700))

		db, err := tsdb.Open(userDir, nil, nil, &tsdb.Options{
			RetentionDuration: int64(time.Hour * 25 / time.Millisecond),
			NoLockfile:        true,
			MinBlockDuration:  chunkRange,
			MaxBlockDuration:  chunkRange,
		}, nil)
		require.NoError(t, err)

		db.DisableCompactions()
		head := db.Head()

		l := labels.Labels{{Name: "n", Value: "v"}}
		for i := 0; i < numFullChunks; i++ {
			// Not using db.Appender() as it checks for compaction.
			app := head.Appender(context.Background())
			_, err := app.Append(0, l, int64(i)*chunkRange+1, 9.99)
			require.NoError(t, err)
			_, err = app.Append(0, l, int64(i+1)*chunkRange, 9.99)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}

		dur := time.Duration(head.MaxTime()-head.MinTime()) * time.Millisecond
		require.True(t, dur > 23*time.Hour)
		require.Equal(t, 0, len(db.Blocks()))
		require.NoError(t, db.Close())
	}

	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = 2 * 24 * time.Hour // Make sure that no newly created blocks are deleted.

	ingester, err := New(ingesterCfg, clientCfg, overrides, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()

	dur := time.Duration(h.MaxTime()-h.MinTime()) * time.Millisecond
	require.True(t, dur <= 2*time.Hour)
	require.Equal(t, 11, len(db.Blocks()))
}

func TestIngester_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}

func TestIngesterNotDeleteUnshippedBlocks(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = time.Millisecond // Which means delete all but first block.
	cfg.IngesterRing.JoinAfter = 0

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.Nil(t, db.Compact())

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Saying that we have shipped the second block, so only that should get deleted.
	require.Nil(t, shipper.WriteMetaFile(nil, db.db.Dir(), &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact())

	// Only the second block should be gone along with a new block.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[0].Meta().ULID, newBlocks[0].Meta().ULID)    // First block remains same.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID)    // 3rd block becomes 2nd now.
	require.NotEqual(t, oldBlocks[1].Meta().ULID, newBlocks[2].Meta().ULID) // The new block won't match previous 2nd block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Shipping 2 more blocks, hence all the blocks from first round.
	require.Nil(t, shipper.WriteMetaFile(nil, db.db.Dir(), &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID, newBlocks[0].Meta().ULID, newBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(6); j < 7; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact())

	// All blocks from the old blocks should be gone now.
	newBlocks2 := db.Blocks()
	require.Equal(t, 2, len(newBlocks2))

	require.Equal(t, newBlocks[2].Meta().ULID, newBlocks2[0].Meta().ULID) // Block created in last round.
	for _, b := range oldBlocks {
		// Second block is not one among old blocks.
		require.NotEqual(t, b.Meta().ULID, newBlocks2[1].Meta().ULID)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks2[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))
}

func TestIngesterPushErrorDuringForcedCompaction(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push a sample, it should succeed.
	pushSingleSampleWithMetadata(t, i)

	// We mock a flushing by setting the boolean.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.casState(active, forceCompacting))

	// Ingestion should fail with a 503.
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = i.Push(ctx, req)
	require.Equal(t, httpgrpc.Errorf(http.StatusServiceUnavailable, wrapWithUser(errors.New("forced compaction in progress"), userID).Error()), err)

	// Ingestion is successful after a flush.
	require.True(t, db.casState(forceCompacting, active))
	pushSingleSampleWithMetadata(t, i)
}

func TestIngesterNoFlushWithInFlightRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push few samples.
	for j := 0; j < 5; j++ {
		pushSingleSampleWithMetadata(t, i)
	}

	// Verifying that compaction won't happen when a request is in flight.

	// This mocks a request in flight.
	db := i.getTSDB(userID)
	require.NoError(t, db.acquireAppendLock())

	// Flush handler only triggers compactions, but doesn't wait for them to finish. We cannot use ?wait=true here,
	// because it would deadlock -- flush will wait for appendLock to be released.
	i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush", nil))

	// Flushing should not have succeeded even after 5 seconds.
	time.Sleep(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// No requests in flight after this.
	db.releaseAppendLock()

	// Let's wait until all head series have been flushed.
	test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
		db := i.getTSDB(userID)
		if db == nil {
			return false
		}
		return db.Head().NumSeries()
	})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 1
	`), "cortex_ingester_tsdb_compactions_total"))
}

func TestIngester_PushInstanceLimits(t *testing.T) {
	tests := map[string]struct {
		limits          InstanceLimits
		reqs            map[string][]*mimirpb.WriteRequest
		expectedErr     error
		expectedErrType interface{}
	}{
		"should succeed creating one user and series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*mimirpb.WriteRequest{
				"test": {
					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}})},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						[]*mimirpb.MetricMetadata{
							{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
						},
						mimirpb.API,
					),
				},
			},
			expectedErr: nil,
		},

		"should fail creating two series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*mimirpb.WriteRequest{
				"test": {
					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),

					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},

			expectedErr: wrapWithUser(errMaxSeriesLimitReached, "test"),
		},

		"should fail creating two users": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*mimirpb.WriteRequest{
				"user1": {
					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),
				},

				"user2": {
					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},
			expectedErr: wrapWithUser(errMaxUsersLimitReached, "user2"),
		},

		"should fail pushing samples in two requests due to rate limit": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1, MaxIngestionRate: 0.001},

			reqs: map[string][]*mimirpb.WriteRequest{
				"user1": {
					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),

					mimirpb.ToWriteRequest(
						[]labels.Labels{mimirpb.FromLabelAdaptersToLabels([]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},
			expectedErr: errMaxSamplesPushRateLimitReached,
		},
	}

	defaultInstanceLimits = nil

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 0
			cfg.InstanceLimitsFn = func() *InstanceLimits {
				return &testData.limits
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// Iterate through users in sorted order (by username).
			uids := []string{}
			totalPushes := 0
			for uid, requests := range testData.reqs {
				uids = append(uids, uid)
				totalPushes += len(requests)
			}
			sort.Strings(uids)

			pushIdx := 0
			for _, uid := range uids {
				ctx := user.InjectOrgID(context.Background(), uid)

				for _, req := range testData.reqs[uid] {
					pushIdx++
					_, err := i.Push(ctx, req)

					if pushIdx < totalPushes {
						require.NoError(t, err)
					} else {
						// Last push may expect error.
						if testData.expectedErr != nil {
							assert.Equal(t, testData.expectedErr, err)
						} else if testData.expectedErrType != nil {
							assert.True(t, errors.As(err, testData.expectedErrType), "expected error type %T, got %v", testData.expectedErrType, err)
						} else {
							assert.NoError(t, err)
						}
					}

					// imitate time ticking between each push
					i.ingestionRate.Tick()

					rate := testutil.ToFloat64(i.metrics.ingestionRate)
					require.NotZero(t, rate)
				}
			}
		})
	}
}

func TestIngester_instanceLimitsMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	l := InstanceLimits{
		MaxIngestionRate:   10,
		MaxInMemoryTenants: 20,
		MaxInMemorySeries:  30,
	}

	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits {
		return &l
	}
	cfg.IngesterRing.JoinAfter = 0

	_, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 0
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 30
		cortex_ingester_instance_limits{limit="max_tenants"} 20
	`), "cortex_ingester_instance_limits"))

	l.MaxInMemoryTenants = 1000
	l.MaxInMemorySeries = 2000

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 0
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 2000
		cortex_ingester_instance_limits{limit="max_tenants"} 1000
	`), "cortex_ingester_instance_limits"))
}

func TestIngester_inflightPushRequests(t *testing.T) {
	limits := InstanceLimits{MaxInflightPushRequests: 1}

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits { return &limits }
	cfg.IngesterRing.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	startCh := make(chan struct{})

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		const targetRequestDuration = time.Second

		samples := 100000
		series := 1

		// Find right series&samples count to make sure that push takes given target duration.
		for {
			req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("test-%d-%d", series, samples)), series, samples)

			start := time.Now()
			_, err := i.Push(ctx, req)
			require.NoError(t, err)

			elapsed := time.Since(start)
			t.Log(series, samples, elapsed)
			if elapsed > targetRequestDuration {
				break
			}

			samples = int(float64(samples) * float64(targetRequestDuration/elapsed) * 1.5) // Adjust number of series to hit our targetRequestDuration push duration.
			for samples >= int(time.Hour.Milliseconds()) {
				// We generate one sample per millisecond, if we have more than an hour of samples TSDB will fail with "out of bounds".
				// So we trade samples for series here.
				samples /= 10
				series *= 10
			}
		}

		// Now repeat push with number of samples calibrated to our target request duration.
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("real-%d-%d", series, samples)), series, samples)

		// Signal that we're going to do the real push now.
		close(startCh)

		_, err := i.Push(ctx, req)
		return err
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
		// failed to setup
		case <-startCh:
			// we can start the test.
		}

		time.Sleep(10 * time.Millisecond) // Give first goroutine a chance to start pushing...
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, "testcase"), 1, 1024)

		_, err := i.Push(ctx, req)
		require.Equal(t, errTooManyInflightPushRequests, err)
		return nil
	})

	require.NoError(t, g.Wait())
}

func generateSamplesForLabel(baseLabels labels.Labels, series, samples int) *mimirpb.WriteRequest {
	lbls := make([]labels.Labels, 0, series*samples)
	ss := make([]mimirpb.Sample, 0, series*samples)

	for s := 0; s < series; s++ {
		l := append(labels.FromStrings("series", strconv.Itoa(s)), baseLabels...)
		for i := 0; i < samples; i++ {
			ss = append(ss, mimirpb.Sample{
				Value:       float64(i),
				TimestampMs: int64(i),
			})
			lbls = append(lbls, l)
		}
	}

	return mimirpb.ToWriteRequest(lbls, ss, nil, nil, mimirpb.API)
}

func buildTestMatrix(numSeries int, samplesPerSeries int, offset int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        model.LabelValue(fmt.Sprintf("testjob%d", i%2)),
			},
			Values: make([]model.SamplePair, 0, samplesPerSeries),
		}
		for j := 0; j < samplesPerSeries; j++ {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(i + j + offset),
				Value:     model.SampleValue(i + j + offset),
			})
		}
		m = append(m, &ss)
	}
	sort.Sort(m)
	return m
}

func matrixToSamples(m model.Matrix) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, mimirpb.Sample{
				TimestampMs: int64(sp.Timestamp),
				Value:       float64(sp.Value),
			})
		}
	}
	return samples
}

// Return one copy of the labels per sample
func matrixToLables(m model.Matrix) []labels.Labels {
	var labels []labels.Labels
	for _, ss := range m {
		for range ss.Values {
			labels = append(labels, mimirpb.FromLabelAdaptersToLabels(mimirpb.FromMetricsToLabelAdapters(ss.Metric)))
		}
	}
	return labels
}

func runTestQuery(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string) (model.Matrix, *client.QueryRequest, error) {
	return runTestQueryTimes(ctx, t, ing, ty, n, v, model.Earliest, model.Latest)
}

func runTestQueryTimes(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string, start, end model.Time) (model.Matrix, *client.QueryRequest, error) {
	matcher, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		return nil, nil, err
	}
	req, err := client.ToQueryRequest(start, end, []*labels.Matcher{matcher})
	if err != nil {
		return nil, nil, err
	}
	s := stream{ctx: ctx}
	err = ing.QueryStream(req, &s)
	require.NoError(t, err)

	res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
	require.NoError(t, err)
	sort.Sort(res)
	return res, req, nil
}

func pushTestMetadata(t *testing.T, ing *Ingester, numMetadata, metadataPerMetric int) ([]string, map[string][]*mimirpb.MetricMetadata) {
	userIDs := []string{"1", "2", "3"}

	// Create test metadata.
	// Map of userIDs, to map of metric => metadataSet
	testData := map[string][]*mimirpb.MetricMetadata{}
	for _, userID := range userIDs {
		metadata := make([]*mimirpb.MetricMetadata, 0, metadataPerMetric)
		for i := 0; i < numMetadata; i++ {
			metricName := fmt.Sprintf("testmetric_%d", i)
			for j := 0; j < metadataPerMetric; j++ {
				m := &mimirpb.MetricMetadata{MetricFamilyName: metricName, Help: fmt.Sprintf("a help for %d", j), Unit: "", Type: mimirpb.COUNTER}
				metadata = append(metadata, m)
			}
		}
		testData[userID] = metadata
	}

	// Append metadata.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, testData[userID], mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func pushTestSamples(t testing.TB, ing *Ingester, numSeries, samplesPerSeries, offset int) ([]string, map[string]model.Matrix) {
	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, samplesPerSeries, i+offset)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), nil, nil, mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func TestIngesterPurgeMetadata(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestMetadata(t, ing, 10, 3)

	time.Sleep(40 * time.Millisecond)
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		ing.purgeUserMetricsMetadata()

		resp, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetMetadata()))
	}
}

func TestIngesterMetadataMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	_, _ = pushTestMetadata(t, ing, 10, 3)

	pushTestMetadata(t, ing, 10, 3)
	pushTestMetadata(t, ing, 10, 3) // We push the _exact_ same metrics again to ensure idempotency. Metadata is kept as a set so there shouldn't be a change of metrics.

	metricNames := []string{
		"cortex_ingester_memory_metadata_created_total",
		"cortex_ingester_memory_metadata_removed_total",
		"cortex_ingester_memory_metadata",
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 90
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
	`), metricNames...))

	time.Sleep(40 * time.Millisecond)
	ing.purgeUserMetricsMetadata()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
		# HELP cortex_ingester_memory_metadata_removed_total The total number of metadata that were removed per user.
		# TYPE cortex_ingester_memory_metadata_removed_total counter
		cortex_ingester_memory_metadata_removed_total{user="1"} 30
		cortex_ingester_memory_metadata_removed_total{user="2"} 30
		cortex_ingester_memory_metadata_removed_total{user="3"} 30
	`), metricNames...))

}

func TestIngesterSendsOnlySeriesWithData(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestSamples(t, ing, 10, 1000, 0)

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, req, err := runTestQueryTimes(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+", model.Latest.Add(-15*time.Second), model.Latest)
		require.NoError(t, err)

		s := stream{
			ctx: ctx,
		}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err)

		// Nothing should be selected.
		require.Equal(t, 0, len(s.responses))
	}

	// Read samples back via chunk store.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

type stream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*client.QueryStreamResponse
}

func (s *stream) Context() context.Context {
	return s.ctx
}

func (s *stream) Send(response *client.QueryStreamResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

// Test that blank labels are removed by the ingester
func TestIngester_Push_SeriesWithBlankLabel(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	lbls := []labels.Labels{{
		{Name: model.MetricNameLabel, Value: "testmetric"},
		{Name: "foo", Value: ""},
		{Name: "bar", Value: ""},
	}}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing.Push(ctx, mimirpb.ToWriteRequest(
		lbls,
		[]mimirpb.Sample{{TimestampMs: 1, Value: 0}},
		nil,
		nil,
		mimirpb.API,
	))
	require.NoError(t, err)

	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, labels.MetricName, "testmetric")

	require.NoError(t, err)
	expected := model.Matrix{
		{
			Metric: model.Metric{labels.MetricName: "testmetric"},
			Values: []model.SamplePair{
				{Timestamp: 1, Value: 0},
			},
		},
	}

	assert.Equal(t, expected, res)
}

func TestIngesterUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerUser = 1
	limits.MaxGlobalMetricsWithMetadataPerUser = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		// Global Ingester limits are computed based on replication factor
		// Set RF=1 here to ensure the series and metadata limits
		// are actually set to 1 instead of 3.
		cfg.IngesterRing.ReplicationFactor = 1
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	// Series
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}
	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric2", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1}, []mimirpb.Sample{sample1}, nil, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append to two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		assert.Equal(t, wrapWithUser(makeLimitError(perUserSeriesLimit, ing.limiter.FormatError(userID, errMaxSeriesPerUserLimitExceeded)), userID).Error(), string(httpResp.Body))

		// Append two metadata, expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(labels1)),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		require.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()

}

func TestIngesterMetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 1
	limits.MaxGlobalMetadataPerMetric = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		// Global Ingester limits are computed based on replication factor
		// Set RF=1 here to ensure the series and metadata limits
		// are actually set to 1 instead of 3.
		cfg.IngesterRing.ReplicationFactor = 1
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1}, []mimirpb.Sample{sample1}, nil, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		assert.Equal(t, wrapWithUser(makeMetricLimitError(perMetricSeriesLimit, labels3, ing.limiter.FormatError(userID, errMaxSeriesPerMetricLimitExceeded)), userID).Error(), string(httpResp.Body))

		// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		// Verify Series
		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(labels1)),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		assert.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()
}

// Construct a set of realistic-looking samples, all with slightly different label sets
func benchmarkData(nSeries int) (allLabels []labels.Labels, allSamples []mimirpb.Sample) {
	// Real example from Kubernetes' embedded cAdvisor metrics, lightly obfuscated.
	var benchmarkLabels = labels.Labels{
		{Name: model.MetricNameLabel, Value: "container_cpu_usage_seconds_total"},
		{Name: "beta_kubernetes_io_arch", Value: "amd64"},
		{Name: "beta_kubernetes_io_instance_type", Value: "c3.somesize"},
		{Name: "beta_kubernetes_io_os", Value: "linux"},
		{Name: "container_name", Value: "some-name"},
		{Name: "cpu", Value: "cpu01"},
		{Name: "failure_domain_beta_kubernetes_io_region", Value: "somewhere-1"},
		{Name: "failure_domain_beta_kubernetes_io_zone", Value: "somewhere-1b"},
		{Name: "id", Value: "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28"},
		{Name: "image", Value: "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506"},
		{Name: "instance", Value: "ip-111-11-1-11.ec2.internal"},
		{Name: "job", Value: "kubernetes-cadvisor"},
		{Name: "kubernetes_io_hostname", Value: "ip-111-11-1-11"},
		{Name: "monitor", Value: "prod"},
		{Name: "name", Value: "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0"},
		{Name: "namespace", Value: "kube-system"},
		{Name: "pod_name", Value: "some-other-name-5j8s8"},
	}

	for j := 0; j < nSeries; j++ {
		labels := benchmarkLabels.Copy()
		for i := range labels {
			if labels[i].Name == "cpu" {
				labels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}
		allLabels = append(allLabels, labels)
		allSamples = append(allSamples, mimirpb.Sample{TimestampMs: 0, Value: float64(j)})
	}
	return
}

type TenantLimitsMock struct {
	mock.Mock
	validation.TenantLimits
}

func (t *TenantLimitsMock) ByUserID(userID string) *validation.Limits {
	returnArgs := t.Called(userID)
	if returnArgs.Get(0) == nil {
		return nil
	}
	return returnArgs.Get(0).(*validation.Limits)
}

func TestIngesterActiveSeries(t *testing.T) {
	labelsToPush := []labels.Labels{
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "false", "team", "a"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "false", "team", "b"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "true", "team", "a"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "true", "team", "b"),
	}

	req := func(lbls labels.Labels, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[]labels.Labels{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			nil,
			mimirpb.API,
		)
	}

	metricNames := []string{
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
	}
	userID := "test_user"
	userID2 := "other_test_user"

	activeSeriesDefaultConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"bool_is_true_flagbased":  `{bool="true"}`,
		"bool_is_false_flagbased": `{bool="false"}`,
	})

	activeSeriesTenantConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"team_a": `{team="a"}`,
		"team_b": `{team="b"}`,
	})

	activeSeriesTenantOverride := new(TenantLimitsMock)
	activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesCustomTrackersConfig: activeSeriesTenantConfig})
	activeSeriesTenantOverride.On("ByUserID", userID2).Return(nil)

	tests := map[string]struct {
		test                func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer)
		reqs                []*mimirpb.WriteRequest
		expectedMetrics     string
		disableActiveSeries bool
	}{
		"successful push, should count active series": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should cleanup metrics when tsdb closed": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
				// close tsdbs and check for cleanup
				ingester.closeAllTSDB()
				expectedMetrics = ""
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should track custom matchers, removing when zero": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Pushing second time to have entires which are not going to be purged
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)

				// Adding time to make the first batch of pushes idle.
				// We update them in the exact moment in time where append time of the first push is already considered idle,
				// while the second append happens after the purge timestamp.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout)
				ingester.updateActiveSeries(currentTime)

				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Update active series again in a further future where no series are active anymore.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout)
				ingester.updateActiveSeries(currentTime)
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(""), metricNames...))
			},
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := ``

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries

			limits := defaultLimitsTestConfig()
			limits.ActiveSeriesCustomTrackersConfig = activeSeriesDefaultConfig
			overrides, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
			require.NoError(t, err)

			ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return ing.lifecycler.HealthyInstancesCount()
			})

			testData.test(t, ing, registry)
		})
	}
}

func TestIngesterActiveSeriesConfigChanges(t *testing.T) {
	labelsToPush := []labels.Labels{
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "false", "team", "a"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "false", "team", "b"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "true", "team", "a"),
		labels.FromStrings(labels.MetricName, "test_metric", "bool", "true", "team", "b"),
	}

	req := func(lbls labels.Labels, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[]labels.Labels{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			nil,
			mimirpb.API,
		)
	}

	metricNames := []string{
		"cortex_ingester_active_series_loading",
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
	}
	userID := "test_user"
	userID2 := "other_test_user"

	activeSeriesDefaultConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"bool_is_true_flagbased":  `{bool="true"}`,
		"bool_is_false_flagbased": `{bool="false"}`,
	})

	activeSeriesTenantConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"team_a": `{team="a"}`,
		"team_b": `{team="b"}`,
	})

	defaultActiveSeriesTenantOverride := new(TenantLimitsMock)
	defaultActiveSeriesTenantOverride.On("ByUserID", userID2).Return(nil)
	defaultActiveSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesCustomTrackersConfig: activeSeriesTenantConfig})

	tests := map[string]struct {
		test               func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer)
		reqs               []*mimirpb.WriteRequest
		expectedMetrics    string
		activeSeriesConfig activeseries.CustomTrackersConfig
		tenantLimits       *TenantLimitsMock
	}{
		"override flag based config with runtime overwrite": {
			tenantLimits:       nil,
			activeSeriesConfig: activeSeriesDefaultConfig,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Add new runtime configs
				activeSeriesTenantOverride := new(TenantLimitsMock)
				activeSeriesTenantOverride.On("ByUserID", userID2).Return(nil)
				activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesCustomTrackersConfig: activeSeriesTenantConfig})
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesCustomTrackersConfig = activeSeriesDefaultConfig
				override, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
				require.NoError(t, err)
				ingester.limits = override
				currentTime = time.Now()
				// First update reloads the config
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
            	    cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"remove runtime overwrite and revert to flag based config": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			tenantLimits:       defaultActiveSeriesTenantOverride,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
            	    cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Remove runtime configs
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesCustomTrackersConfig = activeSeriesDefaultConfig
				override, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"changing runtime override should result in new metrics": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Change runtime configs
				activeSeriesTenantOverride := new(TenantLimitsMock)
				activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesCustomTrackersConfig: mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
					"team_a": `{team="a"}`,
					"team_b": `{team="b"}`,
					"team_c": `{team="b"}`,
					"team_d": `{team="b"}`,
				})})
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesCustomTrackersConfig = activeSeriesDefaultConfig
				override, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_c",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_d",user="test_user"} 2
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should cleanup loading metric at close": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			tenantLimits:       defaultActiveSeriesTenantOverride,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 4
					cortex_ingester_active_series{user="test_user"} 4
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 2
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Remove all configs
				limits := defaultLimitsTestConfig()
				override, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
					cortex_ingester_active_series_loading{user="other_test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
				ingester.closeAllTSDB()
				expectedMetrics = `
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = true

			limits := defaultLimitsTestConfig()
			limits.ActiveSeriesCustomTrackersConfig = testData.activeSeriesConfig
			var overrides *validation.Overrides
			var err error
			// Without this, TenantLimitsMock(nil) != nil when using getOverridesForUser in limits.go
			if testData.tenantLimits != nil {
				overrides, err = validation.NewOverrides(limits, testData.tenantLimits)
				require.NoError(t, err)
			} else {
				overrides, err = validation.NewOverrides(limits, nil)
				require.NoError(t, err)
			}

			ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return ing.lifecycler.HealthyInstancesCount()
			})

			testData.test(t, ing, registry)
		})
	}
}

func pushWithUser(t *testing.T, ingester *Ingester, labelsToPush []labels.Labels, userID string, req func(lbls labels.Labels, t time.Time) *mimirpb.WriteRequest) {
	for _, label := range labelsToPush {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ingester.Push(ctx, req(label, time.Now()))
		require.NoError(t, err)
	}
}

func TestGetIgnoreSeriesLimitForMetricNamesMap(t *testing.T) {
	cfg := Config{}

	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = ", ,,,"
	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = "foo, bar, ,"
	require.Equal(t, map[string]struct{}{"foo": {}, "bar": {}}, cfg.getIgnoreSeriesLimitForMetricNamesMap())
}

func Test_Ingester_QueryOutOfOrder(t *testing.T) {
	tests := map[string]struct {
		queryFrom      int64
		queryTo        int64
		oooFirstSample int64
		oooLastSample  int64
		expPushError   bool
	}{
		"should return in order and out of order data": {
			queryFrom:      math.MinInt64,
			queryTo:        math.MaxInt64,
			oooFirstSample: 70 * time.Minute.Milliseconds(),
			oooLastSample:  99 * time.Minute.Milliseconds(),
			expPushError:   false,
		},
		"if ooo samples go back past allowance writing should return an error": {
			queryFrom:      math.MinInt64,
			queryTo:        math.MaxInt64,
			oooFirstSample: 69 * time.Minute.Milliseconds(), // Allowance is 30 min, first sample is at minute 100 so first ooo sample is too old
			oooLastSample:  99 * time.Minute.Milliseconds(),
			expPushError:   true,
		},
	}

	// Configure ingester with OOO Allowance
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.OOOAllowance = 30 * time.Minute
	cfg.BlocksStorageConfig.TSDB.OOOCapMin = 4
	cfg.BlocksStorageConfig.TSDB.OOOCapMax = 32

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create ingester
			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's healthy
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			ctx := user.InjectOrgID(context.Background(), "test")

			// Push first in-order sample at minute 100
			firstInOrderSample := 100 * time.Minute.Milliseconds()
			serie := series{
				lbls:      labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}},
				value:     float64(firstInOrderSample),
				timestamp: firstInOrderSample,
			}
			expSamples := []model.SamplePair{
				{
					Timestamp: model.Time(firstInOrderSample),
					Value:     model.SampleValue(firstInOrderSample),
				},
			}

			wReq, _, _, _ := mockWriteRequest(t, serie.lbls, serie.value, serie.timestamp)
			_, err = i.Push(ctx, wReq)
			require.NoError(t, err)

			// Push ooo samples within the allowance time in a single request
			var samples []mimirpb.Sample
			var lbls []labels.Labels
			for j := testData.oooFirstSample / time.Minute.Milliseconds(); j < testData.oooLastSample/time.Minute.Milliseconds(); j++ {
				min := int64(j) * time.Minute.Milliseconds()
				samples = append(samples, mimirpb.Sample{
					TimestampMs: min,
					Value:       float64(min),
				})
				lbls = append(lbls, serie.lbls)
				expSamples = append(expSamples, model.SamplePair{
					Timestamp: model.Time(min),
					Value:     model.SampleValue(min),
				})
			}
			wReq = mimirpb.ToWriteRequest(lbls, samples, nil, nil, mimirpb.API)
			_, err = i.Push(ctx, wReq)
			if testData.expPushError {
				require.Error(t, err, "should have failed on push")
				require.ErrorAs(t, err, &storage.ErrTooOldSample)
				return
			}
			require.NoError(t, err)

			// Sort samples by timestamp for later comparison
			sort.Slice(expSamples, func(i, j int) bool {
				return expSamples[i].Timestamp < expSamples[j].Timestamp
			})

			expMatrix := model.Matrix{
				{
					Metric: model.Metric{"__name__": "test_1", "status": "200"},
					Values: expSamples,
				},
			}

			req := &client.QueryRequest{
				StartTimestampMs: testData.queryFrom,
				EndTimestampMs:   testData.queryTo,
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}

			s := stream{ctx: ctx}
			err = i.QueryStream(req, &s)
			require.NoError(t, err)

			res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)
			assert.ElementsMatch(t, expMatrix, res)
		})
	}
}
