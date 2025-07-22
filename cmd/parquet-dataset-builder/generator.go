package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

type DatasetGenerator struct {
	bucket objstore.Bucket
	logger log.Logger
}

func NewDatasetGenerator(bucket objstore.Bucket, logger log.Logger) *DatasetGenerator {
	return &DatasetGenerator{
		bucket: bucket,
		logger: logger,
	}
}

func (g *DatasetGenerator) Generate(ctx context.Context, config *GenerateConfig) (int, error) {
	level.Info(g.logger).Log("msg", "Starting dataset generation")

	tmpDir := filepath.Join(os.TempDir(), "mimir-parquet-dataset-builder")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			level.Warn(g.logger).Log("msg", "Failed to cleanup temp directory", "err", err)
		}
	}()

	series := g.generateSeries(config)
	seriesCount := len(series)
	level.Info(g.logger).Log("msg", "Generated series", "count", seriesCount)

	startTime := time.UnixMilli(0)
	endTime := startTime.Add(time.Duration(config.TimeRangeHours) * time.Hour)

	userBkt := bucket.NewUserBucketClient(config.UserID, g.bucket, nil)

	userDir := filepath.Join(tmpDir, config.UserID)
	if err := os.MkdirAll(userDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create user directory: %w", err)
	}

	// Calculate total samples based on DPM and time range
	totalMinutes := config.TimeRangeHours * 60
	samplesPerSeries := config.DPM * totalMinutes

	level.Info(g.logger).Log("msg", "Generating TSDB blocks", "start_time", startTime, "end_time", endTime, "samples_per_series", samplesPerSeries)
	if err := g.generateTSDBBlocks(ctx, userDir, config.UserID, series, startTime, endTime, samplesPerSeries); err != nil {
		return 0, fmt.Errorf("failed to generate TSDB blocks: %w", err)
	}

	level.Info(g.logger).Log("msg", "Uploading original TSDB blocks to object storage")
	if err := g.uploadTSDBBlocks(ctx, userDir, userBkt); err != nil {
		return 0, fmt.Errorf("failed to upload TSDB blocks: %w", err)
	}

	level.Info(g.logger).Log("msg", "Converting blocks to parquet format")
	converter := NewConverter(g.bucket, g.logger, config.LabelsCompression, config.ChunksCompression, config.LabelsCodec, config.ChunksCodec)
	if err := converter.convertUserBlocks(ctx, config.UserID); err != nil {
		return 0, fmt.Errorf("failed to convert blocks to parquet: %w", err)
	}

	level.Info(g.logger).Log("msg", "Dataset generation completed")
	return seriesCount, nil
}

func (g *DatasetGenerator) generateSeries(config *GenerateConfig) []labels.Labels {
	var series []labels.Labels

	for _, metricName := range config.MetricNames {
		// Generate all possible combinations of label values
		series = append(series, g.generateSeriesForMetric(metricName, config)...)
	}

	return series
}

func (g *DatasetGenerator) generateSeriesForMetric(metricName string, config *GenerateConfig) []labels.Labels {
	var series []labels.Labels

	if len(config.LabelNames) == 0 {
		// If no labels, just create a series with the metric name
		series = append(series, labels.Labels{
			{Name: "__name__", Value: metricName},
		})
		return series
	}

	// Generate all combinations of label values
	combinations := g.generateLabelCombinations(config.LabelNames, config.LabelCardinality)

	for _, combination := range combinations {
		lbls := labels.Labels{
			{Name: "__name__", Value: metricName},
		}

		for i, labelName := range config.LabelNames {
			lbls = append(lbls, labels.Label{Name: labelName, Value: combination[i]})
		}

		series = append(series, lbls)
	}

	return series
}

func (g *DatasetGenerator) generateLabelCombinations(labelNames []string, cardinalities []int) [][]string {
	if len(labelNames) == 0 {
		return [][]string{}
	}

	// Calculate total combinations
	totalCombinations := 1
	for _, cardinality := range cardinalities {
		totalCombinations *= cardinality
	}

	combinations := make([][]string, totalCombinations)

	for i := 0; i < totalCombinations; i++ {
		combination := make([]string, len(labelNames))
		temp := i

		for j := len(labelNames) - 1; j >= 0; j-- {
			combination[j] = fmt.Sprintf("%s_%d", labelNames[j], temp%cardinalities[j])
			temp /= cardinalities[j]
		}

		combinations[i] = combination
	}

	return combinations
}

func (g *DatasetGenerator) generateTSDBBlocks(ctx context.Context, userDir, userID string, series []labels.Labels, minTime, maxTime time.Time, samplesPerSeries int) error {
	tmpDir := filepath.Join(os.TempDir(), "tsdb-"+ulid.MustNew(ulid.Now(), nil).String())
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			level.Warn(g.logger).Log("msg", "Failed to cleanup TSDB temp directory", "err", err)
		}
	}()

	db, err := tsdb.Open(tmpDir, nil, nil, tsdb.DefaultOptions(), nil)
	if err != nil {
		return fmt.Errorf("failed to open TSDB: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			level.Warn(g.logger).Log("msg", "Failed to close TSDB", "err", err)
		}
	}()

	app := db.Appender(ctx)
	step := maxTime.Sub(minTime) / time.Duration(samplesPerSeries)

	level.Info(g.logger).Log("msg", "Adding samples to TSDB", "series_count", len(series), "samples_per_series", samplesPerSeries, "time_step", step)
	for _, s := range series {
		for i := 0; i < samplesPerSeries; i++ {
			ts := minTime.Add(time.Duration(i) * step)
			value := g.generateSampleValue(s.Get("__name__"), ts)
			_, err = app.Append(0, s, timestamp.FromTime(ts), value)
			if err != nil {
				return fmt.Errorf("failed to append sample: %w", err)
			}
		}
	}

	if err := app.Commit(); err != nil {
		return fmt.Errorf("failed to commit samples: %w", err)
	}

	level.Info(g.logger).Log("msg", "Creating TSDB snapshot")
	if err := db.Snapshot(userDir, true); err != nil {
		return fmt.Errorf("failed to snapshot TSDB: %w", err)
	}

	return nil
}

func (g *DatasetGenerator) uploadTSDBBlocks(ctx context.Context, userDir string, userBkt objstore.Bucket) error {
	entries, err := os.ReadDir(userDir)
	if err != nil {
		return fmt.Errorf("failed to read user directory: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		blockID := e.Name()
		blockPath := filepath.Join(userDir, blockID)

		level.Info(g.logger).Log("msg", "Uploading TSDB block", "block_id", blockID)

		// Upload all files in the block directory
		if err := g.uploadBlockDirectory(ctx, blockPath, blockID, userBkt); err != nil {
			return fmt.Errorf("failed to upload block %s: %w", blockID, err)
		}

		level.Info(g.logger).Log("msg", "Successfully uploaded TSDB block", "block_id", blockID)
	}

	return nil
}

func (g *DatasetGenerator) uploadBlockDirectory(ctx context.Context, localBlockPath, blockID string, userBkt objstore.Bucket) error {
	return filepath.Walk(localBlockPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Get relative path from block directory
		relPath, err := filepath.Rel(localBlockPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Create object key
		objectKey := filepath.Join(blockID, relPath)

		// Read file
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer file.Close()

		// Upload file
		if err := userBkt.Upload(ctx, objectKey, file); err != nil {
			return fmt.Errorf("failed to upload file %s: %w", objectKey, err)
		}

		return nil
	})
}

func (g *DatasetGenerator) generateSampleValue(metricName string, t time.Time) float64 {
	switch {
	case strings.Contains(metricName, "cpu"):
		baseValue := 50.0
		timeVariation := 20.0 * (0.5 + 0.5*float64(t.Unix()%3600)/3600.0)
		return baseValue + timeVariation

	case strings.Contains(metricName, "memory"):
		baseValue := 1000000000.0
		trend := float64(t.Unix()%86400) / 86400.0 * 500000000.0
		return baseValue + trend

	case strings.Contains(metricName, "disk"):
		if t.Unix()%300 < 30 {
			return 1000.0 + 500.0*float64(t.Unix()%30)/30.0
		}
		return 50.0

	default:
		return 100.0 + 10.0*float64(t.Unix()%60)/60.0
	}
}
