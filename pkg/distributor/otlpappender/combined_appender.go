// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type CombinedAppender struct {
	series   []mimirpb.PreallocTimeseries
	metadata []*mimirpb.MetricMetadata
}

func NewCombinedAppender() *CombinedAppender {
	return &CombinedAppender{}
}

// GetResult returns the created timeseries and metadata and number of dropped metrics.
func (c *CombinedAppender) GetResult() ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata, int) {
	return c.series, c.metadata, 0
}

func (c *CombinedAppender) AppendSample(ls labels.Labels, meta metadata.Metadata, t, ct int64, v float64, es []exemplar.Exemplar) error {
	idx := len(c.series)-1
	lsAdapter := mimirpb.FromLabelsToLabelAdapters(ls)

	if idx < 0 || mimirpb.CompareLabelAdapters(c.series[idx].Labels, lsAdapter) != 0 || c.series[idx].CreatedTimestamp != ct {
		c.series = append(c.series, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: lsAdapter,
				Samples: []mimirpb.Sample{
					{
						TimestampMs: t,
						Value:       v,
					},
				},
				Exemplars: mimirpb.FromExemplarsToExemplarProtos(es),
				CreatedTimestamp: ct,	
			},
		})
		return nil
	}

	c.series[idx].TimeSeries.Samples = append(c.series[idx].TimeSeries.Samples, mimirpb.Sample{TimestampMs: t, Value: v})
	c.series[idx].TimeSeries.Exemplars = append(c.series[idx].TimeSeries.Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)

	return nil
}

func (c *CombinedAppender) AppendHistogram(ls labels.Labels, meta metadata.Metadata, t, ct int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	// Implementation here
	return nil
}
