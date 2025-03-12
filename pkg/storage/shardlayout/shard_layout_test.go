package shardlayoutpb

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

type cellData struct {
	fileName         string
	numInstances     int
	instanceSizeMiB  int64
	defaultShardSize int
}

func BenchmarkIndexHeaderPartitionSize(b *testing.B) {
	testCases := []struct {
		name string
		cell cellData
	}{
		{
			name: "cortex-prod-13-zone-a",
			cell: cellData{
				"data/cortex-prod-13-zone-a.csv",
				15,
				500 * 1024,
				3 / 3,
			},
		},
	}

	for _, testCase := range testCases {
		allIndexHeadersSorted, tenantIDLookupTable, tenantBinMap := initTestDataFromCSV(
			b, testCase.cell,
		)

		b.ResetTimer()
		b.Run(testCase.name, func(b *testing.B) {
			bins := make([]IndexHeaderPartition, testCase.cell.numInstances)

			bins, minBinSize, maxBinSize := IterBinsBestofNext4FitSpreadWithTenantShard(
				allIndexHeadersSorted, tenantIDLookupTable, bins, testCase.cell.instanceSizeMiB, tenantBinMap)

			//fmt.Println("Max Bin Size:", maxBinSize)
			//fmt.Println("Min Bin Size:", minBinSize)
			//fmt.Println("Diff:", maxBinSize-minBinSize)
			//fmt.Println("Max Bin Size Wasted Space:", testCase.cell.instanceSizeMiB-maxBinSize)
			//fmt.Println("Min Bin Size Wasted Space:", testCase.cell.instanceSizeMiB-minBinSize)

			//for i, bin := range bins {
			//	fmt.Printf("Bin %d: Size: %d\n", i, bin.TotalSizeMiB)
			//	fmt.Printf("\tFirst 5 Headers: %v\n", bin.IndexHeaders[:5])
			//	fmt.Printf("\tLast  5 Headers: %v\n", bin.IndexHeaders[len(bin.IndexHeaders)-5:])
			//}
			totalBinStructSize := 0
			totalBinCompressedStructSize := 0
			for _, bin := range bins {
				totalBinStructSize += bin.Size()
				data, err := bin.Marshal()
				if err != nil {
					b.Fatal(err)
				}

				compressed := snappy.Encode(nil, data)
				totalBinCompressedStructSize += len(compressed)
			}

			minBinSizePct := float64(minBinSize) / float64(testCase.cell.instanceSizeMiB)
			maxBinSizePct := float64(maxBinSize) / float64(testCase.cell.instanceSizeMiB)
			//fmt.Println("TenantBinMap Total Size:", totalSize(tenantBinMap))
			fmt.Println()
			fmt.Println("Bins Total Struct Size MiB:", totalBinStructSize/1024)
			fmt.Println("Bins Total Compressed Struct Size MiB:", totalBinCompressedStructSize/1024)
			fmt.Println()
			b.ReportMetric((maxBinSizePct-minBinSizePct)/float64(len(testCases)), "binSizePctSpread/op")
		})
	}
}

func initTestDataFromCSV(
	t testing.TB,
	cell cellData,
	// cellRing *ring.Ring,
	// instancePrefix string,
	// tenantShardSizeOverrides map[string]int,
	// indexHeadersCache map[string][]IndexHeaderMeta,
) ([]IndexHeaderMeta, []uint32, map[uint32]map[int]struct{}) {
	//indexHeaders, ok := indexHeadersCache[cell.fileName]

	indexHeaders, tenantIDLookups := loadIndexHeadersFromCSV(t, cell.fileName)

	// sort the blocks and build the set of tenants
	tenantBinMap := map[uint32]map[int]struct{}{}
	slices.SortFunc(indexHeaders, func(a, b IndexHeaderMeta) int {
		tenantIDA := tenantIDLookups[a.TenantIDRef]
		tenantIDB := tenantIDLookups[b.TenantIDRef]
		if _, ok := tenantBinMap[tenantIDA]; !ok {
			// for now just build the set of tenants - not assigning bins yet
			tenantBinMap[tenantIDA] = map[int]struct{}{}
		}
		if a.SizeMiB < b.SizeMiB {
			return 1
		} else if a.SizeMiB > b.SizeMiB {
			return -1
		} else {
			// also attempt to sort on other keys to create a stable sort;
			// stable sort allows separate processes to come up with the same results
			if tenantIDA < tenantIDB {
				return 1
			} else if tenantIDA > tenantIDB {
				return -1
			} else {
				return 0
			}
		}
	})

	// for each tenant present, check its shuffle-shard ring for its allowed instance assignments
	//for tenantID := range tenantBinMap {
	//	tenantShardSize := cell.defaultShardSize
	//	if override, ok := tenantShardSizeOverrides[tenantID]; ok {
	//		tenantShardSize = override
	//	}
	//	tenantInstanceList, err := getShuffleShardSubring(cellRing, tenantID, tenantShardSize)
	//	if err != nil {
	//		panic(err)
	//	}
	//	//fmt.Println("Tenant:", tenantID, "Bins:", tenantInstanceList)
	//	for _, instance := range tenantInstanceList {
	//		instanceBinIndex, err := strconv.Atoi(encodedStrings.TrimPrefix(instance, instancePrefix))
	//		require.NoError(t, err)
	//		// our instances end in numbers, equal to the instance indexes to keep it simple for now
	//		tenantBinMap[tenantID][instanceBinIndex] = struct{}{}
	//	}
	//
	//}

	return indexHeaders, tenantIDLookups, tenantBinMap
}

// loadIndexHeadersFromCSV reads dump of the store-gateway index headers from one zone of a cell.
// CSV format is indicated by the header: `Size (MB),Path,Pod`.
// Example data row: `559,/proc/1/root/data/tsdb/229572/01JMFBKRGYNW3A35GXTESRQNGA,store-gateway-zone-a-12`.
func loadIndexHeadersFromCSV(t testing.TB, filename string) ([]IndexHeaderMeta, []uint32) {
	file, err := os.Open(filename)
	defer file.Close()
	require.NoError(t, err)

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	require.NoErrorf(t, err, "failed to read CSV file %s", filename)

	if len(rows) < 2 {
		require.FailNow(t, "file is empty or only contains a header")
	}

	symbolsTable := NewSymbolTable()

	var indexHeaders []IndexHeaderMeta
	for _, row := range rows[1:] {
		sizeMB, err := strconv.Atoi(row[0])
		require.NoError(t, err)

		filePathParts := strings.Split(row[1], "/")
		if len(filePathParts) < 8 {
			// does not match format like `/proc/1/root/data/tsdb/229572/01JMFBKRGYNW3A35GXTESRQNGA`;
			// not an index header directory, likely a parent directory that was not stripped out of CSV
			continue
		}

		blockID := filePathParts[len(filePathParts)-1]
		convertedBlockIDBytes := int64(binary.BigEndian.Uint64([]byte(blockID)))

		tenantID := filePathParts[len(filePathParts)-2]
		tenantIDRef := symbolsTable.Symbolize(tenantID)

		indexHeaders = append(indexHeaders, IndexHeaderMeta{
			BlockID:     convertedBlockIDBytes,
			TenantIDRef: tenantIDRef,
			SizeMiB:     int64(sizeMB),
		})
	}

	return indexHeaders, symbolsTable.Symbols()
}
