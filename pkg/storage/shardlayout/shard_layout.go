package shardlayoutpb

import (
	"encoding/binary"
	"slices"
)

func AddIndexHeader(bin IndexHeaderPartition, newHeader IndexHeaderMeta) IndexHeaderPartition {
	bin.TotalSizeMiB += newHeader.SizeMiB
	bin.IndexHeaders = append(bin.IndexHeaders, newHeader)
	return bin
}

func IterBinsBestofNext4FitSpreadWithTenantShard(
	indexHeaders []IndexHeaderMeta,
	tenantIDLookups []uint32,
	bins []IndexHeaderPartition,
	binSize int64,
	tenantBinMap map[uint32]map[int]struct{},
) ([]IndexHeaderPartition, int64, int64) {
	startBinIndex := 0
	candidateBinIndexes := make([]int, 0, 4)
	// For each indexHeader, find the bin of the next 4 bins *in the tenant shard* with the most space.
	// After each addition, increment / wrap the starting bin index by 1.
	for _, indexHeader := range indexHeaders {
		// find the next 4 bins in the tenant shard
		candidateBinIndexes = candidateBinIndexes[:0]
		tenantID := tenantIDLookups[indexHeader.TenantIDRef]
		tenantBins := tenantBinMap[tenantID]
		checkBinIndex := startBinIndex
		checkedBins, foundBins := 0, 0
		for foundBins < cap(candidateBinIndexes) && checkedBins < len(bins) {
			if _, ok := tenantBins[checkBinIndex]; ok {
				candidateBinIndexes = append(candidateBinIndexes, checkBinIndex)
				foundBins++
			}
			checkedBins++
			checkBinIndex = (checkBinIndex + 1) % len(bins)
		}

		// Find the bin of the next 4 in the tenant shard with the most space.
		// This could be done much more efficiently by having tracked the min candidate bin size
		// while we were collecting the candidate indexes, but this is nice for debugging for now.
		minSizeIfAdded := binSize
		idxMinBinIfAdded := startBinIndex
		for _, candidateIndex := range candidateBinIndexes {
			sizeIfAdded := bins[candidateIndex].TotalSizeMiB + indexHeader.SizeMiB
			if sizeIfAdded < minSizeIfAdded && sizeIfAdded < binSize {
				minSizeIfAdded = sizeIfAdded
				idxMinBinIfAdded = candidateIndex
			}
		}

		// Add to bin
		bin := AddIndexHeader(bins[idxMinBinIfAdded], indexHeader)
		bins[idxMinBinIfAdded] = bin

		startBinIndex = (startBinIndex + 1) % len(bins) // Increment / wrap starting bin index
	}

	minBinSize := slices.MinFunc(bins, func(a, b IndexHeaderPartition) int {
		if a.TotalSizeMiB < b.TotalSizeMiB {
			return -1
		} else if a.TotalSizeMiB > b.TotalSizeMiB {
			return 1
		}
		return 0
	}).TotalSizeMiB
	maxBinSize := slices.MaxFunc(bins, func(a, b IndexHeaderPartition) int {
		if a.TotalSizeMiB < b.TotalSizeMiB {
			return -1
		} else if a.TotalSizeMiB > b.TotalSizeMiB {
			return 1
		}
		return 0
	}).TotalSizeMiB

	return bins, minBinSize, maxBinSize
}

// SymbolsTable implements table for easy symbol use.
type SymbolsTable struct {
	encodedStrings []uint32
	symbolsMap     map[uint32]uint32
}

var emptyStringEncoded uint32

func init() {
	emptyStringEncodedBytes := make([]byte, 32)
	_, _ = binary.Encode(emptyStringEncodedBytes, binary.BigEndian, []byte(""))
	emptyStringEncoded = binary.LittleEndian.Uint32(emptyStringEncodedBytes)
}

// NewSymbolTable returns a symbol table.
func NewSymbolTable() SymbolsTable {
	// Empty string is required as a first element.
	return SymbolsTable{
		symbolsMap:     map[uint32]uint32{emptyStringEncoded: 0},
		encodedStrings: []uint32{emptyStringEncoded},
	}
}

// Symbolize adds (if not added before) a string to the symbols table,
// while returning its reference number.
func (t *SymbolsTable) Symbolize(str string) uint32 {
	strEncodedBytes := make([]byte, 32)
	_, _ = binary.Encode(strEncodedBytes, binary.BigEndian, []byte(""))
	strEncoded := binary.LittleEndian.Uint32(strEncodedBytes)

	if ref, ok := t.symbolsMap[strEncoded]; ok {
		return ref
	}
	ref := uint32(len(t.encodedStrings))
	t.encodedStrings = append(t.encodedStrings, strEncoded)
	t.symbolsMap[strEncoded] = ref
	return ref
}

// Symbols returns computes symbols table to put in e.g. Request.Symbols.
// As per spec, order does not matter.
func (t *SymbolsTable) Symbols() []uint32 {
	return t.encodedStrings
}

// Reset clears symbols table.
func (t *SymbolsTable) Reset() {
	// NOTE: Make sure to keep empty symbol.
	t.encodedStrings = t.encodedStrings[:1]
	for k := range t.symbolsMap {
		if k == emptyStringEncoded {
			continue
		}
		delete(t.symbolsMap, k)
	}
}
