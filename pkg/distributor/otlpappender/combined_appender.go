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
}

type CombinedAppender struct {
	series   []mimirpb.PreallocTimeseries
	metadata []*mimirpb.MetricMetadata
	// To avoid creating extra time series when the same label set is used
	// multiple times, we keep track of the last appended time series.
	refs map[uint64]labelsIdx
	// TODO(krajorama): add overflow for handling hash collisions.
}

func NewCombinedAppender() *CombinedAppender {
	return &CombinedAppender{
		refs: make(map[uint64]labelsIdx),
	}
}

// GetResult returns the created timeseries and metadata and number of dropped metrics.
func (c *CombinedAppender) GetResult() ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata, int) {
	return c.series, c.metadata, 0
}

func (c *CombinedAppender) AppendSample(ls labels.Labels, meta metadata.Metadata, t, ct int64, v float64, es []exemplar.Exemplar) error {
	hash := ls.Hash()
	if idx, ok := c.refs[hash]; ok && labels.Equal(idx.lbls, ls) && c.series[idx.idx].CreatedTimestamp == ct {
		// If the label set already exists, append the sample to the last time series.
		// Unless the created timestamp is different, in which case we create a new time series.
		c.series[idx.idx].TimeSeries.Samples = append(c.series[idx.idx].TimeSeries.Samples, mimirpb.Sample{TimestampMs: t, Value: v})
		if len(es) > 0 {
			c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
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
	c.refs[hash] = labelsIdx{
		idx:  len(c.series) - 1,
		lbls: ls,
	}
	if len(es) > 0 {
		c.series[len(c.series)-1].TimeSeries.Exemplars = append(c.series[len(c.series)-1].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
	}

	if meta.Type != model.MetricTypeUnknown {
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
	hash := ls.Hash()
	if idx, ok := c.refs[hash]; ok && labels.Equal(idx.lbls, ls) && c.series[idx.idx].CreatedTimestamp == ct {
		// If the label set already exists, append the sample to the last time series.
		// Unless the created timestamp is different, in which case we create a new time series.
		c.series[idx.idx].TimeSeries.Histograms = append(c.series[idx.idx].TimeSeries.Histograms, mimirpb.FromHistogramToHistogramProto(t, h))
		if len(es) > 0 {
			c.series[idx.idx].TimeSeries.Exemplars = append(c.series[idx.idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
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
	c.refs[hash] = labelsIdx{
		idx:  len(c.series) - 1,
		lbls: ls,
	}
	if len(es) > 0 {
		c.series[len(c.series)-1].TimeSeries.Exemplars = append(c.series[len(c.series)-1].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
	}

	if meta.Type != model.MetricTypeUnknown {
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
