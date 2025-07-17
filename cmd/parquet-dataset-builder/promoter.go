package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Promoter struct {
	blocksDir string
	logger    log.Logger
}

func NewPromoter(blocksDir string, logger log.Logger) *Promoter {
	return &Promoter{
		blocksDir: blocksDir,
		logger:    logger,
	}
}

func (p *Promoter) PromoteLabels(ctx context.Context) error {
	level.Info(p.logger).Log("msg", "Starting label promotion", "blocks_dir", p.blocksDir)

	// Find all block directories
	blockDirs, err := p.findBlockDirectories()
	if err != nil {
		return fmt.Errorf("failed to find block directories: %w", err)
	}

	level.Info(p.logger).Log("msg", "Found blocks to process", "count", len(blockDirs))

	for _, blockDir := range blockDirs {
		if err := p.promoteLabelsInBlock(ctx, blockDir); err != nil {
			return fmt.Errorf("failed to promote labels in block %s: %w", blockDir, err)
		}
	}

	return nil
}

func (p *Promoter) findBlockDirectories() ([]string, error) {
	var blockDirs []string

	entries, err := os.ReadDir(p.blocksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read blocks directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			blockPath := filepath.Join(p.blocksDir, entry.Name())
			// Check if it's a TSDB block by looking for meta.json
			if _, err := os.Stat(filepath.Join(blockPath, "meta.json")); err == nil {
				blockDirs = append(blockDirs, blockPath)
			}
		}
	}

	return blockDirs, nil
}

func (p *Promoter) promoteLabelsInBlock(ctx context.Context, blockDir string) error {
	level.Info(p.logger).Log("msg", "Processing block", "block", blockDir)

	// Open the block
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(p.logger), blockDir, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return fmt.Errorf("failed to open block %s: %w", blockDir, err)
	}
	defer tsdbBlock.Close()
	q, err := tsdb.NewBlockQuerier(tsdbBlock, 0, math.MaxInt64)
	if err != nil {
		return fmt.Errorf("failed to create querier for block %s: %w", blockDir, err)
	}
	defer q.Close()

	// Find all target_info series and extract their labels
	targetInfoLabels, err := p.extractTargetInfoLabels(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to extract target_info labels: %w", err)
	}

	if len(targetInfoLabels) == 0 {
		level.Info(p.logger).Log("msg", "No target_info series found in block", "block", blockDir)
		return nil
	}

	level.Info(p.logger).Log("msg", "Found target_info series", "count", len(targetInfoLabels), "block", blockDir)

	// Create a new block with promoted labels
	return p.createPromotedBlock(ctx, blockDir, q, targetInfoLabels)
}

func (p *Promoter) extractTargetInfoLabels(ctx context.Context, querier storage.Querier) (map[string]labels.Labels, error) {
	targetInfoLabels := make(map[string]labels.Labels)
	// Get all series with __name__ = "target_info"
	targetInfoMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "target_info")
	seriesSet := querier.Select(ctx, false, nil, targetInfoMatcher)

	for seriesSet.Next() {
		series := seriesSet.At()
		lbls := series.Labels()

		// Extract job and instance labels to create the key
		job := lbls.Get("job")
		instance := lbls.Get("instance")
		if job == "" || instance == "" {
			continue
		}

		key := fmt.Sprintf("%s:%s", job, instance)

		// Store all labels except __name__, job, and instance (they will be used for matching)
		promotedLabels := make(labels.Labels, 0, len(lbls))
		for _, lbl := range lbls {
			if lbl.Name != "__name__" && lbl.Name != "job" && lbl.Name != "instance" {
				promotedLabels = append(promotedLabels, lbl)
			}
		}

		targetInfoLabels[key] = promotedLabels
	}

	if seriesSet.Err() != nil {
		return nil, fmt.Errorf("series set error: %w", seriesSet.Err())
	}

	return targetInfoLabels, nil
}

func (p *Promoter) createPromotedBlock(ctx context.Context, originalBlockDir string, querier storage.Querier, targetInfoLabels map[string]labels.Labels) error {
	// Create a temporary directory for the new block
	tempDir := originalBlockDir + ".promoted"
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to remove temp directory: %w", err)
	}

	// Create new TSDB with proper options
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = 0 // No retention

	newDB, err := tsdb.Open(tempDir, nil, nil, opts, nil)
	if err != nil {
		return fmt.Errorf("failed to create new TSDB: %w", err)
	}
	defer newDB.Close()

	// Get all series from the original block (except target_info)
	allMatcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*")
	seriesSet := querier.Select(ctx, false, nil, allMatcher)

	// Get appender for the new block
	appender := newDB.Appender(ctx)
	defer appender.Rollback()

	for seriesSet.Next() {
		series := seriesSet.At()
		lbls := series.Labels()

		// Skip target_info series - we don't want them in the promoted block
		if lbls.Get("__name__") == "target_info" {
			continue
		}

		// Create promoted labels for this series
		promotedLabels := p.promoteSeriesLabels(lbls, targetInfoLabels)

		// Write the series with promoted labels
		if err := p.writeSeriesWithSamples(appender, promotedLabels, series); err != nil {
			return fmt.Errorf("failed to write series: %w", err)
		}
	}

	if seriesSet.Err() != nil {
		return fmt.Errorf("series set error: %w", seriesSet.Err())
	}

	// Commit the appender
	if err := appender.Commit(); err != nil {
		return fmt.Errorf("failed to commit appender: %w", err)
	}

	// Close the new DB
	if err := newDB.Close(); err != nil {
		return fmt.Errorf("failed to close new DB: %w", err)
	}

	// Replace the original block with the promoted one
	backupDir := originalBlockDir + ".backup"
	if err := os.Rename(originalBlockDir, backupDir); err != nil {
		return fmt.Errorf("failed to backup original block: %w", err)
	}

	if err := os.Rename(tempDir, originalBlockDir); err != nil {
		// Try to restore the backup
		os.Rename(backupDir, originalBlockDir)
		return fmt.Errorf("failed to move promoted block: %w", err)
	}

	// Remove the backup
	if err := os.RemoveAll(backupDir); err != nil {
		level.Warn(p.logger).Log("msg", "Failed to remove backup directory", "dir", backupDir, "err", err)
	}

	level.Info(p.logger).Log("msg", "Successfully promoted labels in block", "block", originalBlockDir)
	return nil
}

func (p *Promoter) promoteSeriesLabels(originalLabels labels.Labels, targetInfoLabels map[string]labels.Labels) labels.Labels {
	// Get job and instance from the original series
	job := originalLabels.Get("job")
	instance := originalLabels.Get("instance")

	if job == "" || instance == "" {
		return originalLabels
	}

	// Look up the target_info labels for this job:instance combination
	key := fmt.Sprintf("%s:%s", job, instance)
	promotedLabels, exists := targetInfoLabels[key]
	if !exists {
		return originalLabels
	}

	// Create a new label set starting with the original labels
	newLabels := make(labels.Labels, 0, len(originalLabels)+len(promotedLabels))

	// First, add all original labels
	for _, lbl := range originalLabels {
		newLabels = append(newLabels, lbl)
	}

	// Then add promoted labels, but only if they don't already exist
	existingLabelNames := make(map[string]bool)
	for _, lbl := range originalLabels {
		existingLabelNames[lbl.Name] = true
	}

	for _, promotedLbl := range promotedLabels {
		if !existingLabelNames[promotedLbl.Name] {
			newLabels = append(newLabels, promotedLbl)
		}
	}

	// Sort the labels
	sort.Slice(newLabels, func(i, j int) bool {
		return newLabels[i].Name < newLabels[j].Name
	})

	return newLabels
}

func (p *Promoter) writeSeriesWithSamples(appender storage.Appender, lbls labels.Labels, series storage.Series) error {
	// Add the series reference
	seriesRef, err := appender.Append(0, lbls, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to add series: %w", err)
	}

	// Iterate through all samples in the series
	iter := series.Iterator(nil)
	for iter.Next() == chunkenc.ValFloat {
		ts, val := iter.At()
		if _, err := appender.Append(seriesRef, lbls, ts, val); err != nil {
			return fmt.Errorf("failed to add sample: %w", err)
		}
	}

	if iter.Err() != nil {
		return fmt.Errorf("series iteration error: %w", iter.Err())
	}

	return nil
}
