package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	kitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/indexheader"
)

var (
	blockDir    = flag.String("block-dir", "", "Path to the TSDB block directory (required)")
	userID      = flag.String("user", "default-user", "User ID for bucket organization")
	verbose     = flag.Bool("verbose", false, "Verbose logging")
	reportSizes = flag.Bool("sizes", true, "Report file sizes after generation")
	forceRegen  = flag.Bool("force", false, "Force regeneration even if files exist")
)

func main() {
	flag.Parse()

	if *blockDir == "" {
		log.Fatal("Error: -block-dir is required")
	}

	// Verify block directory exists and contains required files
	if err := validateBlockDir(*blockDir); err != nil {
		log.Fatalf("Invalid block directory: %v", err)
	}

	// Parse block ID from directory name
	blockName := filepath.Base(*blockDir)
	blockID, err := ulid.Parse(blockName)
	if err != nil {
		log.Fatalf("Invalid block ID in directory name: %v", err)
	}

	if *verbose {
		log.Printf("Generating index headers for block: %s", blockID.String())
		log.Printf("Block directory: %s", *blockDir)
		log.Printf("User ID: %s", *userID)
	}

	if err := generateIndexHeaders(*blockDir, blockID, *userID, *verbose, *reportSizes, *forceRegen); err != nil {
		log.Fatalf("Failed to generate index headers: %v", err)
	}

	if *verbose {
		log.Printf("Index header generation completed successfully")
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

	return nil
}

func generateIndexHeaders(blockDirPath string, blockID ulid.ULID, userID string, verbose, reportSizes, forceRegen bool) error {
	ctx := context.Background()

	// Get the directory structure: we need to create bucket at the right level
	// blockDirPath is like: /path/to/user/BLOCKID
	// We need bucket at: /path/to/ level, then use userBucketClient to access user/BLOCKID
	
	// Extract paths
	userDir := filepath.Dir(blockDirPath)              // /path/to/user
	userName := filepath.Base(userDir)                  // user
	rootDir := filepath.Dir(userDir)                   // /path/to
	
	// Create filesystem bucket at root level
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: rootDir})
	if err != nil {
		return fmt.Errorf("failed to create filesystem bucket: %w", err)
	}
	defer func() { _ = bkt.Close() }()

	// Use the extracted user name instead of the parameter
	userBkt := bucket.NewUserBucketClient(userName, bkt, nil)

	// Check if files already exist (unless force regeneration)
	indexHeaderPath := filepath.Join(blockDirPath, "index-header")
	sparseHeaderPath := filepath.Join(blockDirPath, "sparse-index-header")
	
	if !forceRegen {
		indexExists := false
		sparseExists := false
		
		if _, err := os.Stat(indexHeaderPath); err == nil {
			indexExists = true
		}
		if _, err := os.Stat(sparseHeaderPath); err == nil {
			sparseExists = true
		}
		
		if indexExists && sparseExists {
			if verbose {
				log.Printf("Both index-header and sparse-index-header already exist. Use -force to regenerate.")
			}
			if reportSizes {
				return reportFileSizes(blockDirPath, blockID, verbose)
			}
			return nil
		}
		
		if indexExists {
			if verbose {
				log.Printf("Index-header already exists, will generate sparse-index-header only")
			}
		} else if sparseExists {
			if verbose {
				log.Printf("Sparse-index-header exists but index-header missing, will generate index-header first")
			}
		}
	}

	// Generate index-header if needed or forced
	if forceRegen || !fileExists(indexHeaderPath) {
		if verbose {
			log.Printf("Generating index-header...")
		}
		
		if err := indexheader.WriteBinary(ctx, userBkt, blockID, indexHeaderPath); err != nil {
			return fmt.Errorf("failed to create index-header: %w", err)
		}
		
		if verbose {
			log.Printf("Generated index-header for block %s", blockID.String())
		}
	}

	// Generate sparse-index-header
	if verbose {
		log.Printf("Generating sparse-index-header...")
	}
	
	if err := generateSparseIndexHeader(ctx, blockDirPath, userID, blockID, verbose); err != nil {
		return fmt.Errorf("failed to create sparse-index-header: %w", err)
	}

	// Report file sizes if requested
	if reportSizes {
		return reportFileSizes(blockDirPath, blockID, verbose)
	}

	return nil
}

// generateSparseIndexHeader creates a sparse-index-header from the full index-header
func generateSparseIndexHeader(ctx context.Context, blockDirPath, userID string, blockID ulid.ULID, verbose bool) error {
	logger := kitlog.NewNopLogger()
	if verbose {
		w := kitlog.NewSyncWriter(os.Stderr)
		logger = kitlog.NewLogfmtLogger(w)
	}

	// Create StreamBinaryReader with default metrics (nil registerer creates no-op metrics)
	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)
	
	// Use the user directory (parent of block directory) as the base for StreamBinaryReader
	userDir := filepath.Dir(blockDirPath)
	cfg := indexheader.Config{
		MaxIdleFileHandles: 1,
		VerifyOnLoad:      false,
	}
	
	// Create StreamBinaryReader - this will generate the sparse header automatically if it doesn't exist
	reader, err := indexheader.NewStreamBinaryReader(ctx, logger, nil, userDir, blockID, 32, metrics, cfg)
	if err != nil {
		return fmt.Errorf("failed to create stream binary reader: %w", err)
	}
	defer reader.Close()
	
	if verbose {
		sparseHeaderPath := filepath.Join(blockDirPath, "sparse-index-header")
		if _, err := os.Stat(sparseHeaderPath); err == nil {
			log.Printf("Generated sparse-index-header for block %s", blockID.String())
		} else {
			log.Printf("Warning: sparse-index-header was not created for block %s", blockID.String())
		}
	}
	
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func reportFileSizes(blockDirPath string, blockID ulid.ULID, verbose bool) error {
	if verbose {
		log.Printf("=== File Size Report ===")
	}

	// Files to check
	filesToCheck := []struct {
		name        string
		path        string
		description string
	}{
		{"index", filepath.Join(blockDirPath, "index"), "TSDB index file"},
		{"index-header", filepath.Join(blockDirPath, "index-header"), "Optimized index header"},
		{"sparse-index-header", filepath.Join(blockDirPath, "sparse-index-header"), "Sparse index header"},
	}

	var totalSize int64
	filesReported := 0

	for _, file := range filesToCheck {
		stat, err := os.Stat(file.path)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to stat %s: %w", file.name, err)
			}
			// File doesn't exist
			if verbose {
				log.Printf("  %-20s: %8s (%s)", file.name, "missing", file.description)
			} else {
				log.Printf("%s: missing", file.name)
			}
			continue
		}

		size := stat.Size()
		totalSize += size
		filesReported++

		if verbose {
			log.Printf("  %-20s: %8d bytes (%s)", file.name, size, file.description)
		} else {
			log.Printf("%s: %d bytes", file.name, size)
		}
	}

	// Calculate compression ratios if we have both index and headers
	indexSize := getFileSize(filepath.Join(blockDirPath, "index"))
	headerSize := getFileSize(filepath.Join(blockDirPath, "index-header"))
	sparseSize := getFileSize(filepath.Join(blockDirPath, "sparse-index-header"))

	if verbose && indexSize > 0 && headerSize > 0 && sparseSize > 0 {
		headerRatio := float64(headerSize) / float64(indexSize) * 100
		sparseRatio := float64(sparseSize) / float64(indexSize) * 100
		sparseSaving := float64(headerSize-sparseSize) / float64(headerSize) * 100
		
		log.Printf("  %-20s: %8s", "--- Ratios ---", "")
		log.Printf("  %-20s: %7.1f%% (vs index)", "header reduction", 100-headerRatio)
		log.Printf("  %-20s: %7.1f%% (vs index)", "sparse reduction", 100-sparseRatio)
		log.Printf("  %-20s: %7.1f%% (vs header)", "sparse saving", sparseSaving)
	}

	if verbose && filesReported > 1 {
		log.Printf("  %-20s: %8d bytes", "Total", totalSize)
		log.Printf("========================")
	}

	if filesReported == 0 {
		log.Printf("No files found to report sizes for")
	}

	return nil
}

func getFileSize(path string) int64 {
	if stat, err := os.Stat(path); err == nil {
		return stat.Size()
	}
	return 0
}