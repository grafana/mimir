// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestUnknownHistogramErrors(t *testing.T) {
	var softErrProcessor = NewSoftAppendErrorProcessor(
		func() {}, func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {},
		func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {}, func(string, int64, []mimirpb.LabelAdapter) {},
		func([]mimirpb.LabelAdapter) {}, func([]mimirpb.LabelAdapter) {},
		func(err error, _timestamp int64, _labels []mimirpb.LabelAdapter) bool {
			if errors.Is(err, histogram.ErrHistogramCountNotBigEnough) {
				return true
			} else {
				return false
			}
		},
		func([]mimirpb.LabelAdapter) {},
	)

	require.True(t, softErrProcessor.ProcessErr(histogram.ErrHistogramCountNotBigEnough, 0, []mimirpb.LabelAdapter{}))
	require.False(t, softErrProcessor.ProcessErr(histogram.ErrHistogramCountMismatch, 0, []mimirpb.LabelAdapter{}))
}
