// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

func (h Histogram) IsFloatHistogram() bool {
	return h.GetCountFloat() > 0 || h.GetZeroCountFloat() > 0
}

func (h Histogram) IsGauge() bool {
	return h.ResetHint == Histogram_GAUGE
}
