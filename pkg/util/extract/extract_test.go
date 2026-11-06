// SPDX-License-Identifier: AGPL-3.0-only

package extract

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestUnsafeMetricNameFromLabelAdapters(t *testing.T) {
	storage := []byte("metric")
	borrowed := mimirpb.BorrowString(unsafe.String(unsafe.SliceData(storage), len(storage)))
	name, err := UnsafeMetricNameFromLabelAdapters([]mimirpb.LabelAdapter{
		{Name: mimirpb.BorrowString("other"), Value: mimirpb.BorrowString("value")},
		{Name: mimirpb.BorrowString("__name__"), Value: borrowed},
	})
	require.NoError(t, err)
	require.True(t, name.EqualString("metric"))
	owned := name.Clone()
	copy(storage, "reused")
	require.True(t, name.EqualString("reused"))
	require.Equal(t, "metric", owned)
}

func TestUnsafeMetricNameFromLabelAdaptersMissing(t *testing.T) {
	name, err := UnsafeMetricNameFromLabelAdapters([]mimirpb.LabelAdapter{
		{Name: mimirpb.BorrowString("other"), Value: mimirpb.BorrowString("value")},
	})
	require.ErrorIs(t, err, errNoMetricNameLabel)
	require.True(t, name.IsEmpty())
}
