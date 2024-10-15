// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Testing that TSDB example histogram and expected Sample aligns
func TestSampleHistogram(t *testing.T) {
	for _, i := range []int{0, 1, 3, 100} {
		h := GenerateTestHistogram(i)
		sampleH := mimirpb.FromFloatHistogramToSampleHistogram(h.ToFloat(nil))
		promH := mimirpb.FromMimirSampleToPromHistogram(sampleH)

		require.Equal(t, promH, GenerateTestSampleHistogram(i))
	}
}

func TestSampleFloatHistogram(t *testing.T) {
	for _, i := range []int{0, 1, 3, 100} {
		h := GenerateTestFloatHistogram(i)
		sampleH := mimirpb.FromFloatHistogramToSampleHistogram(h)
		promH := mimirpb.FromMimirSampleToPromHistogram(sampleH)

		require.Equal(t, promH, GenerateTestSampleHistogram(i))
	}
}
