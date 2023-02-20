// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
)

func (h Histogram) IsFloatHistogram() bool {
	return h.GetCountFloat() > 0 || h.GetZeroCountFloat() > 0
}

func (h Histogram) IsGauge() bool {
	return h.ResetHint == Histogram_GAUGE
}

func (h Histogram_ResetHint) ToPrometheusModelType() (histogram.CounterResetHint, error) {
	switch h {
	case Histogram_UNKNOWN:
		return histogram.UnknownCounterReset, nil
	case Histogram_YES:
		return histogram.CounterReset, nil
	case Histogram_NO:
		return histogram.NotCounterReset, nil
	case Histogram_GAUGE:
		return histogram.GaugeType, nil
	default:
		return histogram.UnknownCounterReset, fmt.Errorf("unknown Histogram_ResetHint value %v (%v)", int32(h), h.String())
	}
}

func HistogramResetHintFromPrometheusModelType(h histogram.CounterResetHint) (Histogram_ResetHint, error) {
	switch h {
	case histogram.UnknownCounterReset:
		return Histogram_UNKNOWN, nil
	case histogram.CounterReset:
		return Histogram_YES, nil
	case histogram.NotCounterReset:
		return Histogram_NO, nil
	case histogram.GaugeType:
		return Histogram_GAUGE, nil
	default:
		return Histogram_UNKNOWN, fmt.Errorf("unknown Prometheus counter reset hint value %v", h)
	}
}
