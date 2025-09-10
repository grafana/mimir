// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	otlpappender "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type labelsIdx struct {
	idx  int
	lbls labels.Labels
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
	// multiple times, we keep track of the appended time series.
	// TODO(krajorama): add overflow for handling hash collisions.
	// Do we really need this? The code will just create a new series.
	refs map[uint64]labelsIdx

	// metricFamilies is used to store metadata for each metric family.
	// This is needed to not send metadata duplicates all the time.
	// We could get rid of this if we switched to RW2 all the way through.
	metricFamilies map[string]metadata.Metadata
}

func NewCombinedAppender() *CombinedAppender {
	return &CombinedAppender{
		options: CombinedAppenderOptions{
			ValidIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
		},
		series:         mimirpb.PreallocTimeseriesSliceFromPool(),
		refs:           make(map[uint64]labelsIdx),
		metricFamilies: make(map[string]metadata.Metadata),
	}
}

func (c *CombinedAppender) WithOptions(options CombinedAppenderOptions) *CombinedAppender {
	c.options.EnableCreatedTimestampZeroIngestion = options.EnableCreatedTimestampZeroIngestion
	if options.ValidIntervalCreatedTimestampZeroIngestion > 0 {
		c.options.ValidIntervalCreatedTimestampZeroIngestion = options.ValidIntervalCreatedTimestampZeroIngestion
	} else {
		c.options.ValidIntervalCreatedTimestampZeroIngestion = defaultIntervalForStartTimestamps
	}
	return c
}

// GetResult returns the created timeseries and metadata.
func (c *CombinedAppender) GetResult() ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata) {
	return c.series, c.metadata
}

func (c *CombinedAppender) AppendSample(ls labels.Labels, meta otlpappender.Metadata, ct, t int64, v float64, es []exemplar.Exemplar) error {
	ct = c.recalcCreatedTimestamp(t, ct)

	hash, idx, seenSeries := c.processLabelsAndMetadata(ls)

	if !seenSeries || c.ctRequiresNewSeries(idx.idx, ct) {
		c.createNewSeries(&idx, hash, ls, ct)
	}

	c.series[idx.idx].Samples = append(c.series[idx.idx].Samples, mimirpb.Sample{TimestampMs: t, Value: v})
	c.appendExemplars(idx.idx, es)
	c.appendMetadata(meta.MetricFamilyName, meta.Metadata)

	return nil
}

func (c *CombinedAppender) AppendHistogram(ls labels.Labels, meta otlpappender.Metadata, ct, t int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	ct = c.recalcCreatedTimestamp(t, ct)

	hash, idx, seenSeries := c.processLabelsAndMetadata(ls)

	if !seenSeries || c.ctRequiresNewSeries(idx.idx, ct) {
		c.createNewSeries(&idx, hash, ls, ct)
	}

	c.series[idx.idx].Histograms = append(c.series[idx.idx].Histograms, mimirpb.FromHistogramToHistogramProto(t, h))
	c.appendExemplars(idx.idx, es)
	c.appendMetadata(meta.MetricFamilyName, meta.Metadata)

	return nil
}

func (c *CombinedAppender) recalcCreatedTimestamp(t, ct int64) int64 {
	if !c.options.EnableCreatedTimestampZeroIngestion || ct < 0 || ct > t || (c.options.ValidIntervalCreatedTimestampZeroIngestion > 0 && t-ct > c.options.ValidIntervalCreatedTimestampZeroIngestion) {
		return 0
	}

	return ct
}

// ctRequiresNewSeries checks if the created timestamp is meaningful and different
// from the one already stored in the series at the given index.
func (c *CombinedAppender) ctRequiresNewSeries(seriesIdx int, ct int64) bool {
	return ct > 0 && c.series[seriesIdx].CreatedTimestamp != ct
}

// processLabelsAndMetadata figures out if we have already seen this
// exact label set and whether we need to update the metadata.
// krajorama: I could not make this inline.
func (c *CombinedAppender) processLabelsAndMetadata(ls labels.Labels) (hash uint64, idx labelsIdx, seenSeries bool) {
	hash = ls.Hash()
	idx, ok := c.refs[hash]
	if ok && labels.Equal(idx.lbls, ls) {
		seenSeries = true
	} else {
		idx.lbls = ls
	}
	return
}

func (c *CombinedAppender) createNewSeries(idx *labelsIdx, hash uint64, ls labels.Labels, ct int64) {
	// TODO(krajorama): consider using mimirpb.TimeseriesFromPool
	ts := &mimirpb.TimeSeries{
		Labels:           mimirpb.FromLabelsToLabelAdapters(ls),
		CreatedTimestamp: ct,
	}
	c.series = append(c.series, mimirpb.PreallocTimeseries{TimeSeries: ts})
	idx.idx = len(c.series) - 1
	c.refs[hash] = *idx
}

// appendExemplars appends exemplars to the time series at the given index.
// It's split from appenndMetadata to be eligible for inlining.
func (c *CombinedAppender) appendExemplars(seriesIdx int, es []exemplar.Exemplar) {
	if len(es) == 0 {
		return
	}
	c.series[seriesIdx].Exemplars = append(c.series[seriesIdx].Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
}

// appendMetadata appends metadata to the time series at the given index.
func (c *CombinedAppender) appendMetadata(metricFamilyName string, meta metadata.Metadata) {
	storedMeta, ok := c.metricFamilies[metricFamilyName]
	if ok && storedMeta.Help == meta.Help && storedMeta.Unit == meta.Unit && storedMeta.Type == meta.Type {
		return
	}
	c.metricFamilies[metricFamilyName] = meta

	c.metadata = append(c.metadata, &mimirpb.MetricMetadata{
		Type:             metricTypeToMimirType(meta.Type),
		MetricFamilyName: metricFamilyName,
		Help:             meta.Help,
		Unit:             meta.Unit,
	})
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
