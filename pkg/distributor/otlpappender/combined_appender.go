// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type labelsIdx struct {
	idx  int
	lbls labels.Labels
	meta metadata.Metadata
}

// defaultIntervalForStartTimestamps is hardcoded to 5 minutes in milliseconds.
// Assuming a DPM of 1 and knowing that Grafana's $__rate_interval is typically
// 4 times the write interval that would give us 4 minutes. We add an extra
// minute for delays.
const defaultIntervalForStartTimestamps = int64(300_000)

type CombinedAppenderOptions struct {
	EnableCreatedTimestampZeroIngestion        bool
	ValidIntervalCreatedTimestampZeroIngestion int64
}

type CombinedAppender struct {
	options CombinedAppenderOptions

	series   []mimirpb.PreallocTimeseries
	metadata []*mimirpb.MetricMetadata
	// To avoid creating extra time series when the same label set is used
	// multiple times, we keep track of the last appended time series.
	refs map[uint64]labelsIdx
	// TODO(krajorama): add overflow for handling hash collisions.
}

func NewCombinedAppender(options CombinedAppenderOptions) *CombinedAppender {
	return &CombinedAppender{
		options: options,
		refs:    make(map[uint64]labelsIdx),
	}
}

// GetResult returns the created timeseries and metadata and number of dropped metrics.
func (c *CombinedAppender) GetResult() ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata, int) {
	return c.series, c.metadata, 0
}

func (c *CombinedAppender) AppendSample(ls labels.Labels, meta metadata.Metadata, t, ct int64, v float64, es []exemplar.Exemplar) error {
	if !c.options.EnableCreatedTimestampZeroIngestion {
		// Ignore created timestamps if the feature is disabled.
		ct = 0
	}
	hash := ls.Hash()
	idx, ok := c.refs[hash]
	seenSeries := false // Whether we have seen this series or not.
	appendMeta := false // Whether we append new metadata or not.
	if ok && labels.Equal(idx.lbls, ls) {
		seenSeries = true
		if idx.meta.Type != meta.Type || idx.meta.Help != meta.Help || idx.meta.Unit != meta.Unit {
			// If the label set already exists, but the metadata has changed,
			// we need to update the metadata. Should be rare.
			appendMeta = true
			idx.meta = meta
			c.refs[hash] = idx
		}
	} else {
		// This is a new series.
		appendMeta = true
		idx.lbls = ls
		idx.meta = meta
	}

	if seenSeries && c.series[idx.idx].CreatedTimestamp == ct {
		// If the label set already exists, append the sample to the last time series.
		// Unless the created timestamp is different, in which case we create a new time series.
		c.series[idx.idx].TimeSeries.Samples = append(c.series[idx.idx].TimeSeries.Samples, mimirpb.Sample{TimestampMs: t, Value: v})
		if len(es) > 0 {
			c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
		}

		// If the metric metadata changed, we send an update for it.
		// No need to find and clear the previous metadata, as we always
		// append new metadata.
		if appendMeta {
			c.metadata = append(c.metadata, &mimirpb.MetricMetadata{
				Type:             metricTypeToMimirType(meta.Type),
				MetricFamilyName: ls.Get(model.MetricNameLabel),
				Help:             meta.Help,
				Unit:             meta.Unit,
			})
		}
		return nil
	}

	c.series = append(c.series, mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(ls),
			Samples: []mimirpb.Sample{
				{
					TimestampMs: t,
					Value:       v,
				},
			},
			CreatedTimestamp: ct,
		},
	})
	idx.idx = len(c.series) - 1
	c.refs[hash] = idx

	if len(es) > 0 {
		c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
	}

	if appendMeta {
		c.metadata = append(c.metadata, &mimirpb.MetricMetadata{
			Type:             metricTypeToMimirType(meta.Type),
			MetricFamilyName: ls.Get(model.MetricNameLabel),
			Help:             meta.Help,
			Unit:             meta.Unit,
		})
	}

	return nil
}

func (c *CombinedAppender) AppendHistogram(ls labels.Labels, meta metadata.Metadata, t, ct int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	if !c.options.EnableCreatedTimestampZeroIngestion {
		// Ignore created timestamps if the feature is disabled.
		ct = 0
	}
	hash := ls.Hash()
	idx, ok := c.refs[hash]
	seenSeries := false // Whether we have seen this series or not.
	appendMeta := false // Whether we append new metadata or not.
	if ok && labels.Equal(idx.lbls, ls) {
		seenSeries = true
		if idx.meta.Type != meta.Type || idx.meta.Help != meta.Help || idx.meta.Unit != meta.Unit {
			// If the label set already exists, but the metadata has changed,
			// we need to update the metadata. Should be rare.
			appendMeta = true
			idx.meta = meta
			c.refs[hash] = idx
		}
	} else {
		// This is a new series.
		appendMeta = true
		idx.lbls = ls
		idx.meta = meta
	}

	if seenSeries && c.series[idx.idx].CreatedTimestamp == ct {
		// If the label set already exists, append the sample to the last time series.
		// Unless the created timestamp is different, in which case we create a new time series.
		c.series[idx.idx].TimeSeries.Histograms = append(c.series[idx.idx].TimeSeries.Histograms, mimirpb.FromHistogramToHistogramProto(t, h))
		if len(es) > 0 {
			c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
		}

		// If the metric metadata changed, we send an update for it.
		// No need to find and clear the previous metadata, as we always
		// append new metadata.
		if appendMeta {
			c.metadata = append(c.metadata, &mimirpb.MetricMetadata{
				Type:             metricTypeToMimirType(meta.Type),
				MetricFamilyName: ls.Get(model.MetricNameLabel),
				Help:             meta.Help,
				Unit:             meta.Unit,
			})
		}
		return nil
	}

	c.series = append(c.series, mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(ls),
			Histograms: []mimirpb.Histogram{
				mimirpb.FromHistogramToHistogramProto(t, h),
			},
			CreatedTimestamp: ct,
		},
	})
	idx.idx = len(c.series) - 1
	c.refs[hash] = idx

	if len(es) > 0 {
		c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
	}

	if appendMeta {
		c.metadata = append(c.metadata, &mimirpb.MetricMetadata{
			Type:             metricTypeToMimirType(meta.Type),
			MetricFamilyName: ls.Get(model.MetricNameLabel),
			Help:             meta.Help,
			Unit:             meta.Unit,
		})
	}

	return nil
}

func metricTypeToMimirType(mt model.MetricType) mimirpb.MetricMetadata_MetricType {
	switch mt {
	case model.MetricTypeCounter:
		return mimirpb.COUNTER
	case model.MetricTypeGauge:
		return mimirpb.GAUGE
	case model.MetricTypeHistogram:
		return mimirpb.HISTOGRAM
	case model.MetricTypeGaugeHistogram:
		return mimirpb.GAUGEHISTOGRAM
	case model.MetricTypeSummary:
		return mimirpb.SUMMARY
	case model.MetricTypeInfo:
		return mimirpb.INFO
	case model.MetricTypeStateset:
		return mimirpb.STATESET
	default:
		return mimirpb.UNKNOWN
	}
}
