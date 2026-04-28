// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"

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

type MimirAppender struct {
	EnableCreatedTimestampZeroIngestion        bool
	ValidIntervalCreatedTimestampZeroIngestion int64

	series   []mimirpb.PreallocTimeseries
	metadata []*mimirpb.MetricMetadata
	// To avoid creating extra time series when the same label set is used
	// multiple times, we keep track of the appended time series.
	refs          map[uint64]labelsIdx
	collisionRefs map[uint64][]labelsIdx

	// metricFamilies is used to store metadata for each metric family.
	// This is needed to not send metadata duplicates all the time.
	// We could get rid of this if we switched to RW2 all the way through.
	metricFamilies map[string]metadata.Metadata
}

func NewCombinedAppender() *MimirAppender {
	return &MimirAppender{
		ValidIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
		series:         mimirpb.PreallocTimeseriesSliceFromPool(),
		refs:           make(map[uint64]labelsIdx),
		collisionRefs:  make(map[uint64][]labelsIdx),
		metricFamilies: make(map[string]metadata.Metadata),
	}
}

// GetResult returns the created timeseries and metadata.
func (c *MimirAppender) GetResult() ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata) {
	return c.series, c.metadata
}

func (c *MimirAppender) Append(_ storage.SeriesRef, ls labels.Labels, ct, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) (storage.SeriesRef, error) {
	ct = c.recalcCreatedTimestamp(t, ct)

	hash, idx, collisionIdx, seenSeries := c.processLabelsAndMetadata(ls)

	if !seenSeries || c.ctRequiresNewSeries(idx.idx, ct) {
		c.createNewSeries(&idx, collisionIdx, hash, ls, ct)
	}

	switch {
	case fh != nil:
		c.series[idx.idx].Histograms = append(c.series[idx.idx].Histograms, mimirpb.FromFloatHistogramToHistogramProto(t, fh))
	case h != nil:
		c.series[idx.idx].Histograms = append(c.series[idx.idx].Histograms, mimirpb.FromHistogramToHistogramProto(t, h))
	default:
		c.series[idx.idx].Samples = append(c.series[idx.idx].Samples, mimirpb.Sample{TimestampMs: t, Value: v})
	}
	c.appendExemplars(idx.idx, opts.Exemplars)
	c.appendMetadata(opts.MetricFamilyName, opts.Metadata)

	return 0, nil
}

func (c *MimirAppender) Commit() error   { return nil }
func (c *MimirAppender) Rollback() error { return nil }

func (c *MimirAppender) recalcCreatedTimestamp(t, ct int64) int64 {
	if !c.EnableCreatedTimestampZeroIngestion || ct < 0 || ct > t || (c.ValidIntervalCreatedTimestampZeroIngestion > 0 && t-ct > c.ValidIntervalCreatedTimestampZeroIngestion) {
		return 0
	}

	return ct
}

// ctRequiresNewSeries checks if the created timestamp is meaningful and different
// from the one already stored in the series at the given index.
func (c *MimirAppender) ctRequiresNewSeries(seriesIdx int, ct int64) bool {
	return ct > 0 && c.series[seriesIdx].CreatedTimestamp != ct
}

// processLabelsAndMetadata figures out if we have already seen this
// exact label set and whether we need to update the metadata.
// The returned collisionIdx is -1 if there's no hash collision and
// the index into the collisions otherwise.
// krajorama: I could not make this inline.
func (c *MimirAppender) processLabelsAndMetadata(ls labels.Labels) (hash uint64, idx labelsIdx, collisionIdx int, seenSeries bool) {
	hash = ls.Hash()
	idx, ok := c.refs[hash]
	collisionIdx = -1
	if !ok {
		// No match at all.
		idx.lbls = ls
		return
	}

	if labels.Equal(idx.lbls, ls) {
		// Exact match right away.
		seenSeries = true
		return
	}

	// Match but collision of hash, assume no match and set labels.
	idx.lbls = ls

	// Check if we already stored the colliding labels.
	if collisions, ok := c.collisionRefs[hash]; ok {
		for i, collision := range collisions {
			if labels.Equal(collision.lbls, ls) {
				// Found a stored collision.
				idx.idx = collision.idx
				collisionIdx = i
				seenSeries = true
				return
			}
		}
	}
	// No matching collision, make space for it.
	c.collisionRefs[hash] = append(c.collisionRefs[hash], idx)
	collisionIdx = len(c.collisionRefs[hash]) - 1

	return
}

func (c *MimirAppender) createNewSeries(idx *labelsIdx, collisionIdx int, hash uint64, ls labels.Labels, ct int64) {
	ts := mimirpb.TimeseriesFromPool()
	ts.Labels = mimirpb.FromLabelsToLabelAdapters(ls)
	ts.CreatedTimestamp = ct
	c.series = append(c.series, mimirpb.PreallocTimeseries{TimeSeries: ts})
	idx.idx = len(c.series) - 1

	if collisionIdx == -1 {
		c.refs[hash] = *idx
		return
	}
	c.collisionRefs[hash][collisionIdx] = *idx
}

// appendExemplars appends exemplars to the time series at the given index.
// It's split from appenndMetadata to be eligible for inlining.
func (c *MimirAppender) appendExemplars(seriesIdx int, es []exemplar.Exemplar) {
	if len(es) == 0 {
		return
	}
	c.series[seriesIdx].Exemplars = append(c.series[seriesIdx].Exemplars, mimirpb.FromExemplarsToExemplarProtos(es)...)
}

// appendMetadata appends metadata to the time series at the given index.
func (c *MimirAppender) appendMetadata(metricFamilyName string, meta metadata.Metadata) {
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
