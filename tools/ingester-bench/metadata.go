// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"strconv"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Salts of what decides a metric's metadata, which its own hash does rather than the world's draws, so that a profile
// without metadata generates the same world as before.
const (
	saltMetadataShare  = 0x6d657461_73686172
	saltMetadataLength = 0x6d657461_6c656e67
	saltMetadataChurn  = 0x6d657461_63687572
)

// metricMetadata is the metadata a metric's series send of their own, as Remote Write 2.0 and OTLP do.
type metricMetadata struct {
	value mimirpb.SeriesMetadata
	// Whether its help changes every hour.
	churn bool
}

// unitOf is a hash as a number in [0, 1).
func unitOf(h uint64) float64 {
	return float64(h>>11) / (1 << 53)
}

// metadataOf is the metric's metadata, or nil for a metric whose series send none.
func (w *world) metadataOf(m *metricModel) *metricMetadata {
	p := w.profile
	if p.MetadataShare <= 0 || unitOf(m.hash64(math.MinInt, saltMetadataShare)) >= p.MetadataShare {
		return nil
	}
	length := 40
	if len(p.HelpLength) > 0 {
		length = max(1, int(math.Round(p.HelpLength.Quantile(unitOf(m.hash64(math.MinInt, saltMetadataLength))))))
	}
	value := mimirpb.SeriesMetadata{Help: valueString(w.seed, "help:"+m.name, 0, length)}
	switch m.kind {
	case kindCounter:
		value.Type = mimirpb.COUNTER
	case kindClassicHistogram, kindNativeHistogram:
		value.Type, value.Unit = mimirpb.HISTOGRAM, "seconds"
	default:
		value.Type = mimirpb.GAUGE
	}
	return &metricMetadata{value: value, churn: unitOf(m.hash64(math.MinInt, saltMetadataChurn)) < p.MetadataChurnPerHour}
}

// seriesMetadata is the metadata the slot's series send at t: their metric's, or with UniqueHelp the slot's own, with
// the hour in the help of a churning metric.
func (w *world) seriesMetadata(m *metricModel, slot int, t int64) mimirpb.SeriesMetadata {
	if m.metadata == nil {
		return mimirpb.SeriesMetadata{}
	}
	value := m.metadata.value
	if w.profile.UniqueHelp {
		value.Help += " " + strconv.Itoa(slot)
	}
	if m.metadata.churn {
		value.Help += " (" + strconv.FormatInt(t/3_600_000, 10) + ")"
	}
	return value
}
