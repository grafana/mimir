// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"github.com/prometheus/prometheus/model/labels"
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

	ts := promRW2.TimeSeries{
		LabelsRefs: labelsRefs,
		Samples:    floats,
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
