// SPDX-License-Identifier: AGPL-3.0-only

package rw2util

import (
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// NewWriteRequest is used in testing Remote Write 2.0 to generate a new write request.
func NewWriteRequest() *promRW2.Request {
	return &promRW2.Request{}
}

// AddFloatSeries is used in testing Remote Write 2.0 to add a float series to a write request.
// Write 0 into createdTimestamp to not use it.
func AddFloatSeries(
	req *promRW2.Request,
	lbls labels.Labels,
	floats []promRW2.Sample,
	metricType promRW2.Metadata_MetricType,
	help string,
	unit string,
	createdTimestamp int64,
	exemplars []exemplar.Exemplar,
) *promRW2.Request {
	if req == nil {
		req = NewWriteRequest()
	}

	symBuilder := NewSymbolTableBuilder(req.Symbols)

	labelsRefs := make([]uint32, 0, 2*lbls.Len())
	lbls.Range(func(l labels.Label) {
		labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Name))
		labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Value))
	})

	exemplarsRefs := make([]promRW2.Exemplar, 0, len(exemplars))
	for _, e := range exemplars {
		labelsRefs := make([]uint32, 0, 2*lbls.Len())
		e.Labels.Range(func(l labels.Label) {
			labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Name))
			labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Value))
		})
		exemplarsRefs = append(exemplarsRefs, promRW2.Exemplar{
			LabelsRefs: labelsRefs,
			Value:      e.Value,
			Timestamp:  e.Ts,
		})
	}

	ts := promRW2.TimeSeries{
		LabelsRefs: labelsRefs,
		Samples:    floats,
		Metadata: promRW2.Metadata{
			Type:    metricType,
			HelpRef: symBuilder.GetSymbol(help),
			UnitRef: symBuilder.GetSymbol(unit),
		},
		Exemplars:        exemplarsRefs,
		CreatedTimestamp: createdTimestamp,
	}
	req.Timeseries = append(req.Timeseries, ts)
	req.Symbols = symBuilder.GetSymbols()

	return req
}

// AddHistogramSeries is used in testing Remote Write 2.0 to add a histogram series to a write request.
// Write 0 into createdTimestamp to not use it.
func AddHistogramSeries(
	req *promRW2.Request,
	lbls labels.Labels,
	histograms []promRW2.Histogram,
	help string,
	unit string,
	createdTimestamp int64,
	exemplars []promRW2.Exemplar) *promRW2.Request {
	if req == nil {
		req = NewWriteRequest()
	}

	symBuilder := NewSymbolTableBuilder(req.Symbols)

	var labelsRefs []uint32
	lbls.Range(func(l labels.Label) {
		labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Name))
		labelsRefs = append(labelsRefs, symBuilder.GetSymbol(l.Value))
	})

	metricType := promRW2.Metadata_METRIC_TYPE_HISTOGRAM
	if histograms[0].ResetHint == promRW2.Histogram_RESET_HINT_GAUGE {
		metricType = promRW2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM
	}

	ts := promRW2.TimeSeries{
		LabelsRefs: labelsRefs,
		Histograms: histograms,
		Metadata: promRW2.Metadata{
			Type:    metricType,
			HelpRef: symBuilder.GetSymbol(help),
			UnitRef: symBuilder.GetSymbol(unit),
		},
		Exemplars:        exemplars,
		CreatedTimestamp: createdTimestamp,
	}
	req.Timeseries = append(req.Timeseries, ts)
	req.Symbols = symBuilder.GetSymbols()

	return req
}

type SymbolTableBuilder struct {
	count   uint32
	symbols map[string]uint32
	offset  uint32
	common  map[string]uint32
}

func NewSymbolTableBuilder(symbols []string) *SymbolTableBuilder {
	return NewSymbolTableBuilderWithCommon(symbols, 0, nil)
}

func NewSymbolTableBuilderWithCommon(symbols []string, offset uint32, commonSymbols []string) *SymbolTableBuilder {
	// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
	if len(symbols) == 0 || symbols[0] != "" {
		symbols = append([]string{""}, symbols...)
	}

	symbolsMap := make(map[string]uint32)
	for i, sym := range symbols {
		symbolsMap[sym] = uint32(i) + offset
	}
	commonSymbolsMap := make(map[string]uint32)
	for i, commonSym := range commonSymbols {
		commonSymbolsMap[commonSym] = uint32(i)
	}
	return &SymbolTableBuilder{
		count:   uint32(len(symbols)),
		symbols: symbolsMap,
		offset:  offset,
		common:  commonSymbolsMap,
	}
}

func (symbols *SymbolTableBuilder) GetSymbol(sym string) uint32 {
	if i, ok := symbols.common[sym]; ok {
		return i
	}
	if i, ok := symbols.symbols[sym]; ok {
		return i
	}
	symbols.symbols[sym] = symbols.offset + symbols.count
	symbols.count++
	return symbols.offset + symbols.count - 1
}

func (symbols *SymbolTableBuilder) GetSymbols() []string {
	res := make([]string, len(symbols.symbols))
	for sym, i := range symbols.symbols {
		res[i-symbols.offset] = sym
	}
	return res
}

func FromWriteRequest(req *prompb.WriteRequest) *promRW2.Request {
	var rw2 *promRW2.Request

	for _, ts := range req.Timeseries {
		samples := make([]promRW2.Sample, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			samples = append(samples, promRW2.Sample(s))
		}

		exemplars := make([]exemplar.Exemplar, 0, len(ts.Exemplars))
		for _, e := range ts.Exemplars {
			exemplars = append(exemplars, exemplar.Exemplar{
				Labels: labelsFromPrompb(e.Labels),
				Value:  e.Value,
				Ts:     e.Timestamp,
				HasTs:  e.Timestamp > 0,
			})
		}

		metricType := promRW2.Metadata_METRIC_TYPE_UNSPECIFIED
		help := ""
		unit := ""

		metricName := ts.Labels[slices.IndexFunc(ts.Labels, func(l prompb.Label) bool {
			return l.Name == "__name__"
		})].Value
		metadataIdx := slices.IndexFunc(req.Metadata, func(m prompb.MetricMetadata) bool {
			return m.MetricFamilyName == metricName
		})
		if metadataIdx >= 0 {
			metadata := req.Metadata[metadataIdx]
			switch metadata.Type {
			case prompb.MetricMetadata_COUNTER:
				metricType = promRW2.Metadata_METRIC_TYPE_COUNTER
			case prompb.MetricMetadata_GAUGE:
				metricType = promRW2.Metadata_METRIC_TYPE_GAUGE
			case prompb.MetricMetadata_HISTOGRAM:
				metricType = promRW2.Metadata_METRIC_TYPE_HISTOGRAM
			case prompb.MetricMetadata_SUMMARY:
				metricType = promRW2.Metadata_METRIC_TYPE_SUMMARY
			case prompb.MetricMetadata_GAUGEHISTOGRAM:
				metricType = promRW2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM
			case prompb.MetricMetadata_INFO:
				metricType = promRW2.Metadata_METRIC_TYPE_INFO
			case prompb.MetricMetadata_STATESET:
				metricType = promRW2.Metadata_METRIC_TYPE_STATESET
			default:
				panic(fmt.Errorf("unexpected metadata type: %v", metadata.Type))
			}
			help = metadata.Help
			unit = metadata.Unit
		}

		rw2 = AddFloatSeries(rw2, labelsFromPrompb(ts.Labels), samples, metricType, help, unit, 0, exemplars)
	}

	return rw2
}

func labelsFromPrompb(src []prompb.Label) labels.Labels {
	dst := make([]labels.Label, 0, len(src))
	for _, l := range src {
		dst = append(dst, labels.Label{Name: l.Name, Value: l.Value})
	}
	return labels.New(dst...)
}
