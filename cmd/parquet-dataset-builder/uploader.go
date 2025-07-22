package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"
)

type Uploader struct {
	bucket objstore.Bucket
	logger log.Logger
}

func NewUploader(bucket objstore.Bucket, logger log.Logger) *Uploader {
	return &Uploader{
		bucket: bucket,
		logger: logger,
	}
}

func (u *Uploader) UploadBlocks(ctx context.Context, sourceDir, userID string) (int, error) {
	level.Info(u.logger).Log("msg", "Starting block upload", "source_dir", sourceDir, "user", userID)

	// Walk through the source directory to find all files
	var blocksUploaded int
	err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path from source directory
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		// Create the destination key with user prefix
		destKey := filepath.Join(userID, relPath)
		// Normalize path separators for object storage
		destKey = strings.ReplaceAll(destKey, "\\", "/")

		level.Debug(u.logger).Log("msg", "Uploading file", "source", path, "dest", destKey)

		if err := u.uploadFile(ctx, path, destKey); err != nil {
			return fmt.Errorf("failed to upload file %s: %w", path, err)
		}

		// Check if this is a block directory marker (meta.json indicates a block)
		if filepath.Base(relPath) == "meta.json" {
			blocksUploaded++
			level.Info(u.logger).Log("msg", "Uploaded block", "block_path", filepath.Dir(relPath), "user", userID)
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to walk source directory: %w", err)
	}

	level.Info(u.logger).Log("msg", "Completed block upload", "blocks_uploaded", blocksUploaded, "user", userID)
	return blocksUploaded, nil
}

func (u *Uploader) uploadFile(ctx context.Context, sourcePath, destKey string) error {
	// Open the source file
	file, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer file.Close()

	// Get file info for size
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Upload to bucket
	err = u.bucket.Upload(ctx, destKey, file)
	if err != nil {
		return fmt.Errorf("failed to upload to bucket: %w", err)
	}

	level.Debug(u.logger).Log("msg", "File uploaded successfully", "source", sourcePath, "dest", destKey, "size", info.Size())
	return nil
}