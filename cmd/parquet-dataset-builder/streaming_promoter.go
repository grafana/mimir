package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

// StreamingPromoter implements a memory-efficient promoter that only updates the index
// and metadata while preserving the original chunks directory unchanged.
type StreamingPromoter struct {
	blocksDir string
	logger    log.Logger
}

func NewStreamingPromoter(blocksDir string, logger log.Logger) *StreamingPromoter {
	return &StreamingPromoter{
		blocksDir: blocksDir,
		logger:    logger,
	}
}

func (sp *StreamingPromoter) PromoteLabels(ctx context.Context) error {
	level.Info(sp.logger).Log("msg", "Starting streaming label promotion", "blocks_dir", sp.blocksDir)

	// Find all block directories
	blockDirs, err := sp.findBlockDirectories()
	if err != nil {
		return fmt.Errorf("failed to find block directories: %w", err)
	}

	level.Info(sp.logger).Log("msg", "Found blocks to process", "count", len(blockDirs))

	for _, blockDir := range blockDirs {
		if err := sp.promoteLabelsInBlock(ctx, blockDir); err != nil {
			return fmt.Errorf("failed to promote labels in block %s: %w", blockDir, err)
		}
	}

	return nil
}

func (sp *StreamingPromoter) findBlockDirectories() ([]string, error) {
	var blockDirs []string

	entries, err := os.ReadDir(sp.blocksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read blocks directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			blockPath := filepath.Join(sp.blocksDir, entry.Name())
			// Check if it's a TSDB block by looking for meta.json
			if _, err := os.Stat(filepath.Join(blockPath, "meta.json")); err == nil {
				blockDirs = append(blockDirs, blockPath)
			}
		}
	}

	return blockDirs, nil
}

func (sp *StreamingPromoter) promoteLabelsInBlock(ctx context.Context, blockDir string) error {
	level.Info(sp.logger).Log("msg", "Processing block", "block", blockDir)

	// Open the block
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(sp.logger), blockDir, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return fmt.Errorf("failed to open block %s: %w", blockDir, err)
	}
	defer tsdbBlock.Close()

	// Create a querier to read the block
	q, err := tsdb.NewBlockQuerier(tsdbBlock, 0, math.MaxInt64)
	if err != nil {
		return fmt.Errorf("failed to create querier for block %s: %w", blockDir, err)
	}
	defer q.Close()

	// Extract target_info labels
	targetInfoLabels, err := sp.getTargetInfoLabelsFetcher(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to extract target_info labels: %w", err)
	}

	// Create a new block with promoted labels in the index only
	return sp.createPromotedBlockWithIndexUpdate(ctx, blockDir, tsdbBlock, targetInfoLabels)
}

func (sp *StreamingPromoter) getTargetInfoLabelsFetcher(ctx context.Context, querier storage.Querier) (func(string) labels.Labels, error) {
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

		// Store all labels except __name__, job, and instance
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

	targetInfoLabelsSlice := make([]string, 0, len(targetInfoLabels))
	for key := range targetInfoLabels {
		targetInfoLabelsSlice = append(targetInfoLabelsSlice, key)
	}

	if len(targetInfoLabelsSlice) == 0 {
		level.Info(sp.logger).Log("msg", "No target_info series found in block")
	} else {
		level.Info(sp.logger).Log("msg", "Found target_info series", "count", len(targetInfoLabels))
	}

	getAttributes := func(key string) labels.Labels {
		if lbls, exists := targetInfoLabels[key]; exists {
			return lbls
		}
		h := fnv.New64()
		h.Write([]byte(key))
		index := h.Sum64() % uint64(len(targetInfoLabelsSlice))
		key = targetInfoLabelsSlice[index]
		return targetInfoLabels[key]
	}

	return getAttributes, nil
}

func (sp *StreamingPromoter) createPromotedBlockWithIndexUpdate(
	ctx context.Context,
	originalBlockDir string,
	tsdbBlock *tsdb.Block,
	targetInfoLabels func(string) labels.Labels,
) error {
	outDir := originalBlockDir + ".promoted"
	sp.logger.Log("msg", "Creating directory for promoted block", "temp_dir", outDir)
	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("failed to remove temp directory: %w", err)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Get the index and chunk readers from the original block
	indexReader, err := tsdbBlock.Index()
	if err != nil {
		return fmt.Errorf("failed to get index reader: %w", err)
	}
	defer indexReader.Close()

	chunkReader, err := tsdbBlock.Chunks()
	if err != nil {
		return fmt.Errorf("failed to get chunk reader: %w", err)
	}
	defer chunkReader.Close()

	// Create new index file
	indexPath := filepath.Join(outDir, "index")

	// Create index writer
	indexWriter, err := index.NewWriter(ctx, indexPath)
	if err != nil {
		return fmt.Errorf("failed to create index writer: %w", err)
	}
	defer indexWriter.Close()

	chunksPath := filepath.Join(outDir, "chunks")

	chunkw, err := chunks.NewWriter(chunksPath)
	if err != nil {
		return fmt.Errorf("open chunk writer: %w", err)
	}

	// First, collect all symbols we need (original + any new ones from promotion)
	allSymbols := make(map[string]struct{})

	// Copy all symbols from the original index
	symbols := indexReader.Symbols()
	for symbols.Next() {
		allSymbols[symbols.At()] = struct{}{}
	}
	if symbols.Err() != nil {
		return fmt.Errorf("failed to iterate symbols: %w", symbols.Err())
	}

	// Get all series from the original block first to determine additional symbols needed
	postings, err := indexReader.Postings(ctx, "", "")
	if err != nil {
		return fmt.Errorf("failed to get postings: %w", err)
	}

	var labelsScratch labels.ScratchBuilder
	var chunksMeta []chunks.Meta

	// Store all series data for later processing
	type seriesData struct {
		labels labels.Labels
		chunks []chunks.Meta
	}
	var allSeries []seriesData

	denormalizedSeries := 0
	for postings.Next() {
		seriesRef := postings.At()

		// Read series labels and chunk metadata
		if err := indexReader.Series(seriesRef, &labelsScratch, &chunksMeta); err != nil {
			return fmt.Errorf("failed to read series: %w", err)
		}

		originalLabels := labelsScratch.Labels()

		// Skip target_info series - we don't want them in the promoted block
		// TODO: make parametrizable
		// if originalLabels.Get("__name__") == "target_info" {
		// 	continue
		// }

		// Create promoted labels for this series
		promotedLabels := sp.promoteSeriesLabels(originalLabels, targetInfoLabels)
		if promotedLabels.Len() != len(originalLabels) {
			denormalizedSeries++
			level.Debug(sp.logger).Log("msg", "Promoted labels for series", "original_labels", originalLabels.String(), "promoted_labels", promotedLabels.String())
		}

		// Track any new symbols we need to add
		for _, lbl := range promotedLabels {
			allSymbols[lbl.Name] = struct{}{}
			allSymbols[lbl.Value] = struct{}{}
		}

		// Store the series data
		chunksCopy := make([]chunks.Meta, len(chunksMeta))
		copy(chunksCopy, chunksMeta)
		allSeries = append(allSeries, seriesData{
			labels: promotedLabels,
			chunks: chunksCopy,
		})

		chunksMeta = chunksMeta[:0] // Reset the slice for reuse
	}

	if postings.Err() != nil {
		return fmt.Errorf("failed to iterate postings: %w", postings.Err())
	}

	// Add all symbols first (they must be added in sorted order)
	var symbolsList []string
	for symbol := range allSymbols {
		symbolsList = append(symbolsList, symbol)
	}
	sort.Strings(symbolsList)

	for _, symbol := range symbolsList {
		if err := indexWriter.AddSymbol(symbol); err != nil {
			return fmt.Errorf("failed to add symbol: %w", err)
		}
	}

	slices.SortFunc(allSeries, func(a, b seriesData) int {
		return labels.Compare(a.labels, b.labels)
	})

	// Now add all series
	for i, series := range allSeries {
		chnks := make([]chunks.Meta, 0, len(series.chunks))
		for _, meta := range series.chunks {
			chnk, iterable, err := chunkReader.ChunkOrIterable(meta)
			if err != nil {
				return fmt.Errorf("failed to get chunk for series %d: %w", i, err)
			}
			if iterable != nil {
				return fmt.Errorf("can't handle iterable chunks in this context")
			}
			chnks = append(chnks, chunks.Meta{
				Chunk:   chnk,
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
			})
		}
		err = chunkw.WriteChunks(chnks...)
		if err != nil {
			return fmt.Errorf("failed to write chunks for series %d: %w", i, err)
		}
		if err := indexWriter.AddSeries(storage.SeriesRef(i), series.labels, chnks...); err != nil {
			return fmt.Errorf("failed to add series to index: %w", err)
		}
		if i%100000 == 0 {
			level.Info(sp.logger).Log("msg", "Processed series",
				"series_count", i,
				"denormalized_series", denormalizedSeries,
				"block", originalBlockDir,
				"total_series", len(allSeries))
		}
	}

	seriesCount := len(allSeries)

	// Close the index writer
	if err := indexWriter.Close(); err != nil {
		return fmt.Errorf("failed to close index writer: %w", err)
	}

	// Copy the meta.json file
	originalMetaPath := filepath.Join(originalBlockDir, "meta.json")
	newMetaPath := filepath.Join(outDir, "meta.json")
	if err := sp.copyFile(originalMetaPath, newMetaPath); err != nil {
		return fmt.Errorf("failed to copy meta.json: %w", err)
	}

	// Copy tombstones if they exist
	originalTombstonesPath := filepath.Join(originalBlockDir, "tombstones")
	if _, err := os.Stat(originalTombstonesPath); err == nil {
		newTombstonesPath := filepath.Join(outDir, "tombstones")
		if err := sp.copyFile(originalTombstonesPath, newTombstonesPath); err != nil {
			return fmt.Errorf("failed to copy tombstones: %w", err)
		}
	}

	level.Info(sp.logger).Log("msg", "Successfully promoted labels in block", "block", originalBlockDir, "series", seriesCount)
	return nil
}

func (sp *StreamingPromoter) promoteSeriesLabels(originalLabels labels.Labels, targetInfoLabels func(string) labels.Labels) labels.Labels {
	// Get job and instance from the original series
	job := originalLabels.Get("job")
	instance := originalLabels.Get("instance")

	if job == "" || instance == "" {
		return originalLabels
	}

	// Look up the target_info labels for this job:instance combination
	key := fmt.Sprintf("%s:%s", job, instance)
	promotedLabels := targetInfoLabels(key)

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

func (sp *StreamingPromoter) copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	if _, err := srcFile.WriteTo(dstFile); err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	return nil
}
