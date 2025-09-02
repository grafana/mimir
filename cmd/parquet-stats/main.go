// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/proto"
	"github.com/parquet-go/parquet-go"

	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

func main() {
	var help bool
	var humanReadable bool
	flag.BoolVar(&help, "h", false, "Show help")
	flag.BoolVar(&help, "help", false, "Show help")
	flag.BoolVar(&humanReadable, "human-readable", false, "Output sizes in human-readable format")
	flag.Parse()

	if help || len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s <tsdb-block-path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Outputs sizes of various files in a TSDB block.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	blockPath := flag.Args()[0]

	// Check if block directory exists
	if _, err := os.Stat(blockPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Block directory does not exist: %s\n", blockPath)
		os.Exit(1)
	}

	// Read and display block metadata
	if err := displayBlockMetadata(blockPath, humanReadable); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not read block metadata: %v\n", err)
	}
	fmt.Println()

	// Define the files we want to check
	files := []string{
		"index",
		"index-header",
		"sparse-index-header",
		"0.labels.parquet",
	}

	for _, filename := range files {
		filePath := filepath.Join(blockPath, filename)
		size, err := getFileSize(filePath)
		if err != nil {
			fmt.Printf("%-25s: %s\n", filename, err.Error())
			continue
		}

		if humanReadable {
			fmt.Printf("%-25s: %s\n", filename, humanize.Bytes(uint64(size)))
		} else {
			fmt.Printf("%-25s: %d\n", filename, size)
		}
	}

	// Special handling for parquet footer size
	parquetPath := filepath.Join(blockPath, "0.labels.parquet")
	footerSize, err := getParquetFooterSize(parquetPath)
	if err != nil {
		fmt.Printf("%-25s: %s\n", "0.labels.parquet footer", err.Error())
	} else {
		if humanReadable {
			fmt.Printf("%-25s: %s\n", "0.labels.parquet footer", humanize.Bytes(uint64(footerSize)))
		} else {
			fmt.Printf("%-25s: %d\n", "0.labels.parquet footer", footerSize)
		}
	}

	fmt.Println("\nIn-Memory Sizes:")

	// Calculate sparse index header in-memory size
	sparseHeaderPath := filepath.Join(blockPath, "sparse-index-header")
	sparseMemSize, err := getSparseIndexHeaderMemorySize(sparseHeaderPath)
	if err != nil {
		fmt.Printf("%-25s: %s\n", "sparse-index-header mem", err.Error())
	} else {
		if humanReadable {
			fmt.Printf("%-25s: %s\n", "sparse-index-header mem", humanize.Bytes(uint64(sparseMemSize)))
		} else {
			fmt.Printf("%-25s: %d\n", "sparse-index-header mem", sparseMemSize)
		}
	}

	// Calculate parquet footer in-memory size
	parquetMemSize, err := getParquetFooterMemorySize(parquetPath)
	if err != nil {
		fmt.Printf("%-25s: %s\n", "parquet footer mem", err.Error())
	} else {
		if humanReadable {
			fmt.Printf("%-25s: %s\n", "parquet footer mem", humanize.Bytes(uint64(parquetMemSize)))
		} else {
			fmt.Printf("%-25s: %d\n", "parquet footer mem", parquetMemSize)
		}
	}
}

func getFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("file not found")
	}
	return fileInfo.Size(), nil
}

func getParquetFooterSize(filePath string) (uint32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("error opening file")
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("error getting file info")
	}
	size := fileInfo.Size()

	if size < 8 {
		return 0, fmt.Errorf("file too small")
	}

	// Read the last 8 bytes: [4 bytes footer size][4 bytes "PAR1"]
	footer := make([]byte, 8)
	_, err = file.ReadAt(footer, size-8)
	if err != nil {
		return 0, fmt.Errorf("error reading footer")
	}

	// Verify magic bytes
	if string(footer[4:]) != "PAR1" {
		return 0, fmt.Errorf("invalid parquet file")
	}

	// Extract footer size (little endian)
	footerSize := binary.LittleEndian.Uint32(footer[:4])
	return footerSize, nil
}

type BlockMetadata struct {
	ULID    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
	Stats   struct {
		NumSamples uint64 `json:"numSamples"`
		NumSeries  uint64 `json:"numSeries"`
		NumChunks  uint64 `json:"numChunks"`
	} `json:"stats"`
	Compaction struct {
		Level int `json:"level"`
	} `json:"compaction"`
}

func displayBlockMetadata(blockPath string, humanReadable bool) error {
	metaPath := filepath.Join(blockPath, "meta.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	var meta BlockMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	fmt.Printf("Block ID: %s\n", meta.ULID)
	fmt.Printf("Time Range: %s - %s\n",
		time.Unix(meta.MinTime/1000, 0).UTC().Format("2006-01-02 15:04:05 UTC"),
		time.Unix(meta.MaxTime/1000, 0).UTC().Format("2006-01-02 15:04:05 UTC"))

	if humanReadable {
		fmt.Printf("Samples: %s, Series: %s, Chunks: %s\n",
			humanize.Comma(int64(meta.Stats.NumSamples)),
			humanize.Comma(int64(meta.Stats.NumSeries)),
			humanize.Comma(int64(meta.Stats.NumChunks)))
	} else {
		fmt.Printf("Samples: %d, Series: %d, Chunks: %d\n",
			meta.Stats.NumSamples,
			meta.Stats.NumSeries,
			meta.Stats.NumChunks)
	}

	fmt.Printf("Compaction Level: %d\n", meta.Compaction.Level)

	return nil
}

func getSparseIndexHeaderMemorySize(filePath string) (int64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read file")
	}

	// Decompress the sparse index header (it's gzip-compressed)
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("failed to create gzip reader")
	}
	defer gzipReader.Close()

	decompressed, err := io.ReadAll(gzipReader)
	if err != nil {
		return 0, fmt.Errorf("failed to decompress")
	}

	// Parse the protobuf structure
	sparseHeader := &indexheaderpb.Sparse{}
	if err := proto.Unmarshal(decompressed, sparseHeader); err != nil {
		return 0, fmt.Errorf("failed to parse sparse header protobuf")
	}

	// Calculate deep memory size of the parsed structure
	return calculateStructSize(reflect.ValueOf(sparseHeader).Elem()), nil
}

func getParquetFooterMemorySize(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file")
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info")
	}

	// Open the parquet file and parse metadata (this also loads column/offset indexes by default)
	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet file")
	}

	// Calculate total memory size including metadata, column indexes, and offset indexes
	totalSize := int64(0)
	
	// Get the metadata structure
	metadata := parquetFile.Metadata()
	totalSize += calculateStructSize(reflect.ValueOf(metadata).Elem())
	
	// Add column indexes
	columnIndexes := parquetFile.ColumnIndexes()
	totalSize += calculateStructSize(reflect.ValueOf(columnIndexes))
	
	// Add offset indexes  
	offsetIndexes := parquetFile.OffsetIndexes()
	totalSize += calculateStructSize(reflect.ValueOf(offsetIndexes))
	
	return totalSize, nil
}

func calculateStructSize(v reflect.Value) int64 {
	size := int64(v.Type().Size())

	switch v.Kind() {
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			size += calculateStructSize(v.Index(i))
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			size += calculateStructSize(v.Index(i))
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			size += calculateStructSize(key)
			size += calculateStructSize(v.MapIndex(key))
		}
	case reflect.Ptr:
		if !v.IsNil() {
			size += calculateStructSize(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if field.CanInterface() {
				size += calculateStructSize(field)
			}
		}
	case reflect.String:
		size += int64(len(v.String()))
	case reflect.Interface:
		if !v.IsNil() {
			size += calculateStructSize(v.Elem())
		}
	case reflect.Chan, reflect.Func:
		// These are pointers, size already accounted for in v.Type().Size()
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		 reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		 reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		// These are basic types, size already accounted for in v.Type().Size()
	}

	return size
}
