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

// Promoter implements a memory-efficient promoter that only updates the index
// and metadata while preserving the original chunks directory unchanged.
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

func (sp *Promoter) PromoteLabels(ctx context.Context) error {
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

func (sp *Promoter) findBlockDirectories() ([]string, error) {
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

func (sp *Promoter) promoteLabelsInBlock(ctx context.Context, blockDir string) error {
	level.Info(sp.logger).Log("msg", "Processing block", "block", blockDir)

	// Open the block
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(sp.logger), blockDir, nil, tsdb.DefaultPostingsDecoderFactory,
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

	targetInfoLabels, err := sp.getTargetInfoLabelsFetcher(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to extract target_info labels: %w", err)
	}

	return sp.createPromotedBlockWithIndexUpdate(ctx, blockDir, tsdbBlock, targetInfoLabels)
}

func (sp *Promoter) getTargetInfoLabelsFetcher(ctx context.Context, querier storage.Querier) (func(labels.Labels) labels.Labels, error) {
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
		level.Info(sp.logger).Log("msg", "Found target_info series", "unique_job_instance_count", len(targetInfoLabels))
	}

	getAttributes := func(originalLabels labels.Labels) labels.Labels {
		job := originalLabels.Get("job")
		instance := originalLabels.Get("instance")

		if job == "" || instance == "" {
			return originalLabels
		}

		key := fmt.Sprintf("%s:%s", job, instance)
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

func (sp *Promoter) createPromotedBlockWithIndexUpdate(
	ctx context.Context,
	originalBlockDir string,
	tsdbBlock *tsdb.Block,
	targetInfoLabels func(labels.Labels) labels.Labels,
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
		if err := indexReader.Series(seriesRef, &labelsScratch, &chunksMeta); err != nil {
			return fmt.Errorf("failed to read series: %w", err)
		}

		originalLabels := labelsScratch.Labels()

		// Skip target_info series - we don't want them in the promoted block
		// TODO: make parametrizable
		if originalLabels.Get("__name__") == "target_info" {
			continue
		}

		promotedLabels := sp.promoteSeriesLabels(originalLabels, targetInfoLabels)
		if promotedLabels.Len() != len(originalLabels) {
			denormalizedSeries++
			level.Debug(sp.logger).Log("msg", "Promoted labels for series", "original_labels", originalLabels.String(), "promoted_labels", promotedLabels.String())
		}

		for _, lbl := range promotedLabels {
			allSymbols[lbl.Name] = struct{}{}
			allSymbols[lbl.Value] = struct{}{}
		}

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

	if err := indexWriter.Close(); err != nil {
		return fmt.Errorf("failed to close index writer: %w", err)
	}

	originalMetaPath := filepath.Join(originalBlockDir, "meta.json")
	newMetaPath := filepath.Join(outDir, "meta.json")
	if err := copyFile(originalMetaPath, newMetaPath); err != nil {
		return fmt.Errorf("failed to copy meta.json: %w", err)
	}

	originalTombstonesPath := filepath.Join(originalBlockDir, "tombstones")
	if _, err := os.Stat(originalTombstonesPath); err == nil {
		newTombstonesPath := filepath.Join(outDir, "tombstones")
		if err := copyFile(originalTombstonesPath, newTombstonesPath); err != nil {
			return fmt.Errorf("failed to copy tombstones: %w", err)
		}
	}

	level.Info(sp.logger).Log("msg", "Successfully promoted labels in block", "block", originalBlockDir, "series", seriesCount)
	return nil
}

func (sp *Promoter) promoteSeriesLabels(originalLabels labels.Labels, targetInfoLabels func(labels.Labels) labels.Labels) labels.Labels {
	promotedLabels := targetInfoLabels(originalLabels)
	if promotedLabels.Len() == 0 {
		return originalLabels
	}

	newLabels := make(labels.Labels, 0, len(originalLabels)+len(promotedLabels))

	for _, lbl := range originalLabels {
		newLabels = append(newLabels, lbl)
	}

	existingLabelNames := make(map[string]bool)
	for _, lbl := range originalLabels {
		existingLabelNames[lbl.Name] = true
	}

	for _, promotedLbl := range promotedLabels {
		if !existingLabelNames[promotedLbl.Name] {
			newLabels = append(newLabels, promotedLbl)
		}
	}

	sort.Slice(newLabels, func(i, j int) bool {
		return newLabels[i].Name < newLabels[j].Name
	})

	return newLabels
}

func copyFile(src, dst string) error {
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
