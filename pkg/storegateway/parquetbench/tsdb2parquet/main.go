package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

var (
	blockDir     = flag.String("block-dir", "", "Path to the TSDB block directory to convert (required)")
	outputDir    = flag.String("output", "", "Output directory for converted Parquet files (defaults to same as input)")
	userID       = flag.String("user", "converted-user", "User ID for the converted Parquet block")
	compression  = flag.Bool("compression", true, "Enable compression for Parquet data")
	sortBy       = flag.String("sort-by", "", "Comma-separated list of fields to sort by in Parquet data")
	verbose      = flag.Bool("verbose", false, "Verbose logging")
	reportSizes  = flag.Bool("sizes", true, "Report file sizes after conversion")
)

type blockMeta struct {
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`
}

func main() {
	flag.Parse()

	if *blockDir == "" {
		log.Fatal("Error: -block-dir is required")
	}

	// Verify block directory exists and contains required files
	if err := validateBlockDir(*blockDir); err != nil {
		log.Fatalf("Invalid block directory: %v", err)
	}

	// Set output directory to input if not specified
	outputPath := *outputDir
	if outputPath == "" {
		outputPath = filepath.Dir(*blockDir)
	}

	if *verbose {
		log.Printf("Converting TSDB block: %s", *blockDir)
		log.Printf("Output directory: %s", outputPath)
		log.Printf("User ID: %s", *userID)
		log.Printf("Compression: %t", *compression)
		if *sortBy != "" {
			log.Printf("Sort by: %s", *sortBy)
		}
	}

	if err := convertBlock(*blockDir, outputPath, *userID, *compression, *sortBy, *verbose, *reportSizes); err != nil {
		log.Fatalf("Conversion failed: %v", err)
	}

	if *verbose {
		log.Printf("Conversion completed successfully")
	}
}

func validateBlockDir(dir string) error {
	// Check if directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", dir)
	}

	// Check for required files
	requiredFiles := []string{"index", "meta.json"}
	for _, file := range requiredFiles {
		filePath := filepath.Join(dir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Errorf("required file missing: %s", file)
		}
	}

	// Check for chunks directory
	chunksDir := filepath.Join(dir, "chunks")
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		return fmt.Errorf("chunks directory missing: %s", chunksDir)
	}

	return nil
}

func convertBlock(blockDirPath, outputDir, userID string, compressionEnabled bool, sortByStr string, verbose, reportSizes bool) error {
	ctx := context.Background()

	// Parse block ID from directory name
	blockName := filepath.Base(blockDirPath)
	blockID, err := ulid.Parse(blockName)
	if err != nil {
		return fmt.Errorf("invalid block ID in directory name: %w", err)
	}

	// Read block metadata to get time range
	meta, err := readBlockMeta(filepath.Join(blockDirPath, "meta.json"))
	if err != nil {
		return fmt.Errorf("failed to read block metadata: %w", err)
	}

	if verbose {
		log.Printf("Block ID: %s", blockID.String())
		log.Printf("Time range: %d - %d", meta.MinTime, meta.MaxTime)
	}

	// Open the TSDB block
	tsdbBlock, err := tsdb.OpenBlock(nil, blockDirPath, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to open TSDB block: %w", err)
	}
	defer func() {
		if err := tsdbBlock.Close(); err != nil {
			log.Printf("Warning: failed to close TSDB block: %v", err)
		}
	}()

	// Create filesystem bucket for output
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: outputDir})
	if err != nil {
		return fmt.Errorf("failed to create filesystem bucket: %w", err)
	}
	defer func() { _ = bkt.Close() }()

	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	// Parse sort-by labels
	var sortByLabels []string
	if sortByStr != "" {
		fields := strings.Split(sortByStr, ",")
		for _, field := range fields {
			if trimmed := strings.TrimSpace(field); trimmed != "" {
				sortByLabels = append(sortByLabels, trimmed)
			}
		}
	}

	// Configure conversion options
	convertOpts := []convert.ConvertOption{
		convert.WithName(blockID.String()),
		convert.WithLabelsCompression(schema.WithCompressionEnabled(compressionEnabled)),
		convert.WithChunksCompression(schema.WithCompressionEnabled(compressionEnabled)),
	}

	if len(sortByLabels) > 0 {
		convertOpts = append(convertOpts, convert.WithSortBy(sortByLabels...))
	}

	if verbose {
		log.Printf("Starting conversion to Parquet...")
	}

	// Convert the TSDB block to Parquet
	_, err = convert.ConvertTSDBBlock(
		ctx,
		userBkt,
		meta.MinTime,
		meta.MaxTime,
		[]convert.Convertible{tsdbBlock},
		convertOpts...,
	)
	if err != nil {
		return fmt.Errorf("failed to convert block: %w", err)
	}

	// Write conversion mark
	if err := parquetconverter.WriteConversionMark(ctx, blockID, userBkt); err != nil {
		return fmt.Errorf("failed to write conversion mark: %w", err)
	}

	if verbose {
		log.Printf("Parquet conversion completed")
	}

	// Report file sizes if requested
	if reportSizes {
		if err := reportFileSizes(outputDir, userID, blockID, verbose); err != nil {
			log.Printf("Warning: failed to report file sizes: %v", err)
		}
	}

	return nil
}

func readBlockMeta(metaPath string) (*blockMeta, error) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}

	var meta blockMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// footerSize returns the size of the Parquet footer from the last 8 bytes of the file
func footerSize(filename string) (uint32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}

	size := fi.Size()
	if size < 8 {
		return 0, fmt.Errorf("file too small to contain footer")
	}

	// Last 8 bytes of the file
	buf := make([]byte, 8)
	_, err = file.ReadAt(buf, size-8)
	if err != nil {
		return 0, err
	}

	footerLength := binary.LittleEndian.Uint32(buf[0:4])
	return footerLength, nil
}

func reportFileSizes(outputDir, userID string, blockID ulid.ULID, verbose bool) error {
	blockPath := filepath.Join(outputDir, userID, blockID.String())

	if verbose {
		log.Printf("=== File Size Report ===")
	}

	// Files to check
	filesToCheck := []struct {
		name        string
		path        string
		description string
	}{
		{"0.labels.parquet", filepath.Join(blockPath, "0.labels.parquet"), "Parquet labels file"},
		{"0.chunks.parquet", filepath.Join(blockPath, "0.chunks.parquet"), "Parquet chunks file"},
		{"parquet-conversion-mark.json", filepath.Join(blockPath, "parquet-conversion-mark.json"), "Conversion mark file"},
	}

	var totalSize int64
	filesReported := 0

	for _, file := range filesToCheck {
		stat, err := os.Stat(file.path)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to stat %s: %w", file.name, err)
			}
			// File doesn't exist, which might be normal for some files
			continue
		}

		size := stat.Size()
		totalSize += size
		filesReported++

		if verbose {
			log.Printf("  %-30s: %8d bytes (%s)", file.name, size, file.description)
		} else {
			log.Printf("%s: %d bytes", file.name, size)
		}

		// For Parquet labels file, also report footer size
		if file.name == "0.labels.parquet" {
			footerLen, err := footerSize(file.path)
			if err != nil {
				if verbose {
					log.Printf("  %-30s: %8s (%s)", "  - labels footer", "error", "Could not read footer size")
				} else {
					log.Printf("0.labels.parquet footer: error reading footer size")
				}
			} else {
				if verbose {
					log.Printf("  %-30s: %8d bytes (%s)", "  - labels footer", footerLen, "Parquet footer metadata")
				} else {
					log.Printf("0.labels.parquet footer: %d bytes", footerLen)
				}
			}
		}

		// For Parquet chunks file, also report footer size
		if file.name == "0.chunks.parquet" {
			footerLen, err := footerSize(file.path)
			if err != nil {
				if verbose {
					log.Printf("  %-30s: %8s (%s)", "  - chunks footer", "error", "Could not read footer size")
				} else {
					log.Printf("0.chunks.parquet footer: error reading footer size")
				}
			} else {
				if verbose {
					log.Printf("  %-30s: %8d bytes (%s)", "  - chunks footer", footerLen, "Parquet footer metadata")
				} else {
					log.Printf("0.chunks.parquet footer: %d bytes", footerLen)
				}
			}
		}
	}

	if verbose && filesReported > 1 {
		log.Printf("  %-30s: %8d bytes", "Total", totalSize)
		log.Printf("========================================")
	}

	if filesReported == 0 {
		log.Printf("No Parquet files found to report sizes for")
	}

	return nil
}