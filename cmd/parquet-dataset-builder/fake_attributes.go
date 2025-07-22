package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
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

type AttributesGenerator struct {
	cfg    AttributesGeneratorConfig
	logger log.Logger

	attributes map[string][]string

	jobInstanceSeen map[string]struct{}
}

func NewAttributesGenerator(cfg AttributesGeneratorConfig, logger log.Logger) *AttributesGenerator {
	attributes := make(map[string][]string)
	for instance, cardinality := range cfg.Cardinalities {
		attributes[instance] = randomStrings(instance, cardinality)
	}
	return &AttributesGenerator{
		cfg:             cfg,
		logger:          logger,
		attributes:      attributes,
		jobInstanceSeen: make(map[string]struct{}),
	}
}

func (ag *AttributesGenerator) GenerateAttributes(ctx context.Context) error {
	level.Info(ag.logger).Log("msg", "Processing block", "block", ag.cfg.BlockDirectory)

	// Open the block
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(ag.logger), ag.cfg.BlockDirectory, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return fmt.Errorf("failed to open block %s: %w", ag.cfg.BlockDirectory, err)
	}
	defer tsdbBlock.Close()

	outDir := ag.cfg.BlockDirectory + ".attributes"

	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
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

	var labelsScratch labels.ScratchBuilder
	var chunksMeta []chunks.Meta

	// Store all series data for later processing
	type seriesData struct {
		labels labels.Labels
		chunks []chunks.Meta
	}
	var allSeries []seriesData

	// Get all series from the original block first to determine additional symbols needed
	postings, err := indexReader.Postings(ctx, "", "")
	if err != nil {
		return fmt.Errorf("failed to get postings: %w", err)
	}

	for postings.Next() {
		seriesRef := postings.At()
		if err := indexReader.Series(seriesRef, &labelsScratch, &chunksMeta); err != nil {
			return fmt.Errorf("failed to read series: %w", err)
		}

		lbls := labelsScratch.Labels()
		if lbls.Get("__name__") == "target_info" {
			// Discard
			level.Info(ag.logger).Log("msg", "Discarding target_info series", "series", lbls)
			continue
		}

		if ag.cfg.Promote {
			lbls = ag.extendWithAttributes(lbls)
			level.Info(ag.logger).Log("msg", "Extended labels with attributes",
				"original_labels", labelsScratch.Labels().String(),
				"extended_labels", lbls.String())
		} else {
			// TODO: add a target_info series
			panic("Not promoting is not supported yet")
		}

		for _, lbl := range lbls {
			allSymbols[lbl.Name] = struct{}{}
			allSymbols[lbl.Value] = struct{}{}
		}

		chunksCopy := make([]chunks.Meta, len(chunksMeta))
		copy(chunksCopy, chunksMeta)
		allSeries = append(allSeries, seriesData{
			labels: lbls,
			chunks: chunksCopy,
		})

		chunksMeta = chunksMeta[:0]
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
			level.Info(ag.logger).Log("msg", "Processed series",
				"series_count", i,
				"block", ag.cfg.BlockDirectory,
				"total_series", len(allSeries))
		}
	}

	seriesCount := len(allSeries)

	if err := indexWriter.Close(); err != nil {
		return fmt.Errorf("failed to close index writer: %w", err)
	}

	originalMetaPath := filepath.Join(ag.cfg.BlockDirectory, "meta.json")
	newMetaPath := filepath.Join(outDir, "meta.json")
	if err := copyFile(originalMetaPath, newMetaPath); err != nil {
		return fmt.Errorf("failed to copy meta.json: %w", err)
	}

	originalTombstonesPath := filepath.Join(ag.cfg.BlockDirectory, "tombstones")
	if _, err := os.Stat(originalTombstonesPath); err == nil {
		newTombstonesPath := filepath.Join(outDir, "tombstones")
		if err := copyFile(originalTombstonesPath, newTombstonesPath); err != nil {
			return fmt.Errorf("failed to copy tombstones: %w", err)
		}
	}

	level.Info(ag.logger).Log("msg", "Successfully promoted labels in block", "block", ag.cfg.BlockDirectory, "series", seriesCount, "unique_job_instance_pairs", len(ag.jobInstanceSeen))
	return nil
}

func (ag *AttributesGenerator) extendWithAttributes(originalLabels labels.Labels) labels.Labels {
	job := originalLabels.Get("job")
	instance := originalLabels.Get("instance")

	// Nothing to do
	if job == "" || instance == "" {
		return originalLabels
	}

	newLabels := make(labels.Labels, 0, len(originalLabels)+len(ag.attributes))
	for _, lbl := range originalLabels {
		newLabels = append(newLabels, lbl)
	}

	key := fmt.Sprintf("%s_%s", job, instance)
	ag.jobInstanceSeen[key] = struct{}{}
	for attr, choices := range ag.attributes {
		if newLabels.Get(attr) != "" {
			// If the label already exists, skip it
			continue
		}
		newLabels = append(newLabels, labels.Label{
			Name:  attr,
			Value: choices[hashString(key)%uint64(len(choices))],
		})
	}

	sort.Slice(newLabels, func(i, j int) bool {
		return newLabels[i].Name < newLabels[j].Name
	})

	return newLabels
}

func randomStrings(seed string, count int) []string {
	const strlen = 10
	h := fnv.New64a()
	h.Write([]byte(seed))
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	strs := make([]string, count)
	for i := 0; i < count; i++ {
		b := make([]rune, strlen)
		for j := range b {
			b[j] = letters[r.Intn(len(letters))]
		}
		strs[i] = string(b)
	}
	return strs
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
