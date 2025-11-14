// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"strconv"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	rw2util "github.com/grafana/mimir/pkg/util/test"
)

// Measure unmarshal performance between Remote Write 1.0 and 2.0.
// Testing with large data sets, use "-benchtime 5s" to get more accurate results.
func BenchmarkUnMarshal(b *testing.B) {
	const numSeries = 10000

	const numFamilies = 100 // Number of unique metric families.

	// Some labels are common, like cluster/namespace, etc.
	// Some labels are unique, like pod_name, container_name, etc.
	const numCommonLabels = 30
	const numUniqueLabels = 30
	// Number of exemplars per series. This is not going to be true for native
	// histograms, but it's a good approximation.
	const numExemplars = 1
	const numExemplarLabels = 5

	// Generate a random series in Remote Write 1.0 format.
	rw1Request := &WriteRequest{
		Timeseries: make([]PreallocTimeseries, numSeries),
		Metadata:   make([]*MetricMetadata, numFamilies),
	}

	for i := 0; i < numSeries; i++ {
		rw1Request.Timeseries[i] = PreallocTimeseries{TimeSeries: &TimeSeries{}}
		rw1Request.Timeseries[i].Labels = generateLabels("", i, numCommonLabels, numUniqueLabels)

		rw1Request.Timeseries[i].Samples = make([]Sample, 1)
		// Histograms are the same in both formats so we skip them.
		rw1Request.Timeseries[i].Samples[0].Value = 1.0

		rw1Request.Timeseries[i].Exemplars = make([]Exemplar, numExemplars)
		for j := 0; j < numExemplars; j++ {
			rw1Request.Timeseries[i].Exemplars[j].Labels = generateLabels("exemplar_", i, 0, numExemplarLabels)
			rw1Request.Timeseries[i].Exemplars[j].Value = 2.0
		}
	}
	for i := 0; i < numFamilies; i++ {
		rw1Request.Metadata[i] = &MetricMetadata{}
		rw1Request.Metadata[i].MetricFamilyName = fmt.Sprintf("metric_%d", i)
		rw1Request.Metadata[i].Help = fmt.Sprintf("help_%d", i)
		rw1Request.Metadata[i].Unit = fmt.Sprintf("unit_%d", i)
		rw1Request.Metadata[i].Type = COUNTER
	}

	// Convert RW1 to RW2.
	rw2Request := &WriteRequest{}
	symBuilder := rw2util.NewSymbolTableBuilder(nil)

	for i, ts := range rw1Request.Timeseries {
		rw2Ts := TimeSeriesRW2{}
		for _, label := range ts.Labels {
			rw2Ts.LabelsRefs = append(rw2Ts.LabelsRefs, symBuilder.GetSymbol(label.Name))
			rw2Ts.LabelsRefs = append(rw2Ts.LabelsRefs, symBuilder.GetSymbol(label.Value))
		}
		for _, sample := range ts.Samples {
			rw2Ts.Samples = append(rw2Ts.Samples, Sample{Value: sample.Value})
		}
		// Histograms are the same in both formats so we skip them.
		for _, exemplar := range ts.Exemplars {
			rw2Exemplar := ExemplarRW2{
				Value: exemplar.Value,
			}
			for _, label := range exemplar.Labels {
				rw2Exemplar.LabelsRefs = append(rw2Exemplar.LabelsRefs, symBuilder.GetSymbol(label.Name))
				rw2Exemplar.LabelsRefs = append(rw2Exemplar.LabelsRefs, symBuilder.GetSymbol(label.Value))
			}
			rw2Ts.Exemplars = append(rw2Ts.Exemplars, rw2Exemplar)
		}
		rw2Ts.Metadata.Type = METRIC_TYPE_COUNTER
		rw2Ts.Metadata.HelpRef = symBuilder.GetSymbol(rw1Request.Metadata[i%numFamilies].Help)
		rw2Ts.Metadata.UnitRef = symBuilder.GetSymbol(rw1Request.Metadata[i%numFamilies].Unit)

		rw2Request.TimeseriesRW2 = append(rw2Request.TimeseriesRW2, rw2Ts)
	}
	rw2Request.SymbolsRW2 = symBuilder.GetSymbols()
	require.Len(b, rw2Request.SymbolsRW2, 1+numCommonLabels*2+numSeries*numUniqueLabels*2+numFamilies*2+numExemplarLabels*numSeries*numExemplars*2)

	b.Run("Marshal/RW1", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := rw1Request.Marshal()
			require.NoError(b, err)
			require.NotEmpty(b, data)
		}
	})

	b.Run("Marshal/RW2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := rw2Request.Marshal()
			require.NoError(b, err)
			require.NotEmpty(b, data)
		}
	})

	rw1Data, err := rw1Request.Marshal()
	require.NoError(b, err)
	require.NotEmpty(b, rw1Data)

	rw2Data, err := rw2Request.Marshal()
	require.NoError(b, err)
	require.NotEmpty(b, rw2Data)

	for _, skipExemplars := range []bool{true, false} {
		b.Run(fmt.Sprintf("Unmarshal/RW1/skipExemplars=%v", skipExemplars), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pw := PreallocWriteRequest{}
				pw.SkipUnmarshalingExemplars = skipExemplars
				err := pw.Unmarshal(rw1Data)
				require.NoError(b, err)
			}
		})

		b.Run(fmt.Sprintf("Unmarshal/RW2/skipExemplars=%v", skipExemplars), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pw := PreallocWriteRequest{}
				pw.SkipUnmarshalingExemplars = skipExemplars
				pw.UnmarshalFromRW2 = true
				err := pw.Unmarshal(rw2Data)
				require.NoError(b, err)
			}
		})
	}
}

func generateLabels(prefix string, seriesNumber, numCommonLabels, numUniqueLabels int) []LabelAdapter {
	labels := make([]promlabels.Label, numCommonLabels+numUniqueLabels)
	for i := 0; i < numCommonLabels; i++ {
		labels[i].Name = prefix + "common_label_" + strconv.Itoa(i)
		labels[i].Value = prefix + "common_value_" + strconv.Itoa(i)
	}
	for i := 0; i < numUniqueLabels; i++ {
		idx := numCommonLabels + i
		uid := seriesNumber*(numUniqueLabels) + i
		labels[idx].Name = prefix + "unique_label_" + strconv.Itoa(uid)
		labels[idx].Value = prefix + "unique_value_" + strconv.Itoa(uid)
	}
	return FromLabelsToLabelAdapters(promlabels.New(labels...))
}
