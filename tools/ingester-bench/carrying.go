// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Carrying is what carrying per-series metadata costs a profile's records, which the on/off pairs leave out by design,
// as their records carry it either way: bytes per sample with the series' own metadata, and without it, as today's
// senders' records go, with their families' metadata still as stand-ins. And for a profile with resources, what
// carrying them costs.
type Carrying struct {
	WithBytesPerSample    float64           `json:"with_bytes_per_sample"`
	WithoutBytesPerSample float64           `json:"without_bytes_per_sample"`
	Increase              float64           `json:"increase"`
	Resources             *ResourceCarrying `json:"resources,omitempty"`
}

// ResourceCarrying is what carrying the series' resources costs the records: bytes per sample with them and without
// them, the target_info series written either way, and the share of the records' bytes their resource tables are. And
// what two other encodings would make the records, estimated from them: symbolized, the attributes' keys and string
// values in the record's symbols, and with the series' refs in a field numbered below 16, which takes a byte less.
type ResourceCarrying struct {
	WithBytesPerSample       float64 `json:"with_bytes_per_sample"`
	WithoutBytesPerSample    float64 `json:"without_bytes_per_sample"`
	Increase                 float64 `json:"increase"`
	TableShare               float64 `json:"table_share"`
	SymbolizedBytesPerSample float64 `json:"symbolized_bytes_per_sample"`
	SymbolizedIncrease       float64 `json:"symbolized_increase"`
	LowFieldBytesPerSample   float64 `json:"low_field_bytes_per_sample"`
	LowFieldIncrease         float64 `json:"low_field_increase"`
}

// carryingCost produces the seconds of the profile's records twice, with and without the series' own metadata, and
// compares their bytes, and for a profile with resources, again without the resources. The feed is deterministic, so
// all are of the same series and samples.
func carryingCost(profile Profile, series int, seconds int64) (Carrying, error) {
	start := time.Now().UnixMilli()/60_000*60_000 - 3_600_000
	produce := func(withoutMetadata, withoutResources bool, encodings *encodingEstimate) (float64, error) {
		f, err := newFeed(newWorld(profile.Series, series, 1), "", 0)
		if err != nil {
			return 0, err
		}
		defer f.close()
		f.withoutSeriesMetadata, f.withoutResources, f.encodings = withoutMetadata, withoutResources, encodings
		if err := f.produceSeconds(context.Background(), start, start+seconds*1000); err != nil {
			return 0, err
		}
		if encodings != nil {
			encodings.samples = float64(f.samples.Load())
		}
		return float64(f.recordBytes.Load()) / float64(f.samples.Load()), nil
	}
	var encodings *encodingEstimate
	if profile.Series.Resources != nil {
		encodings = &encodingEstimate{}
	}
	with, err := produce(false, false, encodings)
	if err != nil {
		return Carrying{}, err
	}
	without, err := produce(true, false, nil)
	if err != nil {
		return Carrying{}, err
	}
	c := Carrying{WithBytesPerSample: with, WithoutBytesPerSample: without, Increase: with/without - 1}
	if encodings == nil {
		return c, nil
	}
	withoutResources, err := produce(false, true, nil)
	if err != nil {
		return Carrying{}, err
	}
	symbolized := with - encodings.symbolizedSaving/encodings.samples
	lowField := with - encodings.seriesWithRefs/encodings.samples
	c.Resources = &ResourceCarrying{
		WithBytesPerSample: with, WithoutBytesPerSample: withoutResources, Increase: with/withoutResources - 1,
		TableShare:               encodings.tableBytes / (with * encodings.samples),
		SymbolizedBytesPerSample: symbolized, SymbolizedIncrease: symbolized/withoutResources - 1,
		LowFieldBytesPerSample: lowField, LowFieldIncrease: lowField/withoutResources - 1,
	}
	return c, nil
}

// encodingEstimate adds up, over the records produced, their resource tables' bytes, the series with a resource ref,
// and the bytes symbolizing the tables would save.
type encodingEstimate struct {
	mu                                           sync.Mutex
	tableBytes, seriesWithRefs, symbolizedSaving float64
	samples                                      float64
}

// add estimates a record's other encodings, from its content. A symbolized attribute refers to its key and string
// value by their indexes into the record's symbols, which a string not among them yet is added to, after the
// common symbols, the series' labels and the metadata's help and unit.
func (e *encodingEstimate) add(content []byte, version int) error {
	var req mimirpb.PreallocWriteRequest
	if err := ingest.DeserializeRecordContent(content, &req, version); err != nil {
		return err
	}
	symbols := map[string]int{}
	symbol := func(s string) (ref int, added bool) {
		if ref, ok := symbols[s]; ok {
			return ref, false
		}
		symbols[s] = ingest.V2RecordSymbolOffset + len(symbols)
		return symbols[s], true
	}
	for i, s := range ingest.V2CommonSymbols.GetSlice() {
		symbols[s] = i
	}
	var refs float64
	for _, ts := range req.Timeseries {
		for _, l := range ts.Labels {
			symbol(l.Name)
			symbol(l.Value)
		}
		symbol(ts.SeriesMetadata.Help)
		symbol(ts.SeriesMetadata.Unit)
		if ts.ResourceRef != 0 {
			refs++
		}
	}
	var table, saving int
	for _, entry := range req.ResourceTable {
		size, symbolized := 0, 0
		for i := range entry.Attributes {
			a := &entry.Attributes[i]
			attribute := a.Size()
			size += 1 + sov(attribute) + attribute
			// The key and the string value become varint refs, and new symbols, each its tag, length and bytes.
			smaller := attribute - stringField(a.Key) - stringField(a.StringValue)
			ref, added := symbol(a.Key)
			smaller += 1 + sov(ref)
			if added {
				saving -= stringField(a.Key)
			}
			if a.StringValue != "" {
				ref, added := symbol(a.StringValue)
				smaller += 1 + sov(ref)
				if added {
					saving -= stringField(a.StringValue)
				}
			}
			symbolized += 1 + sov(smaller) + smaller
		}
		// Each entry is field 1002 of the request, a tag of 2 bytes.
		table += 2 + sov(size) + size
		saving += sov(size) + size - sov(symbolized) - symbolized
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tableBytes += float64(table)
	e.seriesWithRefs += refs
	e.symbolizedSaving += float64(saving)
	return nil
}

// stringField is the bytes of a string field of a message: its tag, length and bytes, none for an empty one.
func stringField(s string) int {
	if s == "" {
		return 0
	}
	return 1 + sov(len(s)) + len(s)
}

// sov is the bytes of n as a varint.
func sov(n int) int {
	size := 1
	for v := uint64(n); v >= 0x80; v >>= 7 {
		size++
	}
	return size
}
