package main

import (
	"github.com/grafana/mimir/pkg/mimirpb"
)

// NOTE: this has not been implemented to be efficient.
func minimizeWriteRequest(req *mimirpb.WriteRequest) *mimirpb.MinimizedWriteRequest {
	minReq := mimirpb.MinimizedWriteRequest{}
	minReq.Source = int32(req.Source)
	minReq.Metadata = req.Metadata
	minReq.SkipLabelNameValidation = req.SkipLabelNameValidation

	// Build the symbols table.
	var (
		symbols    string
		symbolsMap = map[string]uint32{}
	)

	minReq.Timeseries = make([]mimirpb.MinimizedTimeSeries, 0, len(req.Timeseries))

	for _, ts := range req.Timeseries {
		minTs := mimirpb.MinimizedTimeSeries{
			LabelSymbols: make([]uint32, 0, len(ts.Labels)*2),
			Samples:      ts.Samples,
			Exemplars:    ts.Exemplars,
			Histograms:   ts.Histograms,
		}

		for _, lbl := range ts.Labels {
			// Label name.
			if ref, ok := symbolsMap[lbl.Name]; ok {
				minTs.LabelSymbols = append(minTs.LabelSymbols, ref)
			} else {
				ref = packRef(len(symbols), len(lbl.Name))
				symbolsMap[lbl.Name] = ref
				symbols += lbl.Name
				minTs.LabelSymbols = append(minTs.LabelSymbols, ref)
			}

			// Label value.
			if ref, ok := symbolsMap[lbl.Value]; ok {
				minTs.LabelSymbols = append(minTs.LabelSymbols, ref)
			} else {
				ref = packRef(len(symbols), len(lbl.Value))
				symbolsMap[lbl.Value] = ref
				symbols += lbl.Value
				minTs.LabelSymbols = append(minTs.LabelSymbols, ref)
			}
		}

		minReq.Timeseries = append(minReq.Timeseries, minTs)
	}

	minReq.Symbols = symbols

	return &minReq
}

func packRef(offset, length int) uint32 {
	return uint32(((offset & 0xFFFFF) << 12) | (length & 0x1FFF))
}

func unpackRef(ref uint32) (offset, length int) {
	return int(ref >> 12), int(ref & 0x1FFF)
}

// NOTE: this has not been implemented to be efficient.
func hyperMinimizeWriteRequest(req *mimirpb.WriteRequest) *mimirpb.HyperMinimizedWriteRequest {
	minReq := mimirpb.HyperMinimizedWriteRequest{}
	minReq.Source = int32(req.Source)
	minReq.Metadata = req.Metadata
	minReq.SkipLabelNameValidation = req.SkipLabelNameValidation

	// Build the symbols table.
	var (
		nextSymbolID = 0
		symbolsMap   = map[string]int{}
	)

	minReq.Timeseries = make([]mimirpb.HyperMinimizedTimeSeries, 0, len(req.Timeseries))

	for _, ts := range req.Timeseries {
		minTs := mimirpb.HyperMinimizedTimeSeries{
			Samples:    ts.Samples,
			Exemplars:  ts.Exemplars,
			Histograms: ts.Histograms,
		}

		for _, lbl := range ts.Labels {
			// Label name.
			if id, ok := symbolsMap[lbl.Name]; ok {
				minTs.LabelSymbolIds = append(minTs.LabelSymbolIds, uint32(id))
			} else {
				// Add symbol to the table.
				id = nextSymbolID
				symbolsMap[lbl.Name] = id
				minReq.SymbolOffsets = append(minReq.SymbolOffsets, uint32(len(minReq.Symbols)))
				minReq.SymbolLength = append(minReq.SymbolLength, uint32(len(lbl.Name)))
				minReq.Symbols += lbl.Name
				nextSymbolID++

				minTs.LabelSymbolIds = append(minTs.LabelSymbolIds, uint32(id))
			}

			// Label value.
			if id, ok := symbolsMap[lbl.Value]; ok {
				minTs.LabelSymbolIds = append(minTs.LabelSymbolIds, uint32(id))
			} else {
				// Add symbol to the table.
				id = nextSymbolID
				symbolsMap[lbl.Value] = id
				minReq.SymbolOffsets = append(minReq.SymbolOffsets, uint32(len(minReq.Symbols)))
				minReq.SymbolLength = append(minReq.SymbolLength, uint32(len(lbl.Value)))
				minReq.Symbols += lbl.Value
				nextSymbolID++

				minTs.LabelSymbolIds = append(minTs.LabelSymbolIds, uint32(id))
			}
		}

		minReq.Timeseries = append(minReq.Timeseries, minTs)
	}

	return &minReq
}
