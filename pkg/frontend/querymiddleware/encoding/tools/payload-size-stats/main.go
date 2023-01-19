// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding"
	intmath "github.com/grafana/mimir/pkg/util/math"
)

func main() {
	examplesDir := ""
	flag.StringVar(&examplesDir, "examples-dir", "", "Directory containing sample JSON query result payloads")
	flag.Parse()

	if err := run(examplesDir); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run(examplesDir string) error {
	if examplesDir == "" {
		return errors.New("examples-dir flag is required")
	}

	examplePayloads, err := loadPayloads(examplesDir)
	if err != nil {
		return err
	}

	stats := map[string]codecStats{}

	for name, codec := range encoding.KnownCodecs {
		s, err := calculateStatsForCodec(codec, examplePayloads)
		if err != nil {
			return fmt.Errorf("could not calculate statistics for codec '%v': %w", name, err)
		}

		stats[name] = s
	}

	fmt.Println("Name\tMinimum size (bytes)\tMaximum size (bytes)\tAverage size (bytes)\tTotal size (bytes)")

	for name, s := range stats {
		fmt.Printf("%v\t%v\t%v\t%.1f\t%v\n", name, s.minSize, s.maxSize, s.avgSize, s.totalSize)
	}

	return nil
}

func loadPayloads(examplesDir string) ([]querymiddleware.PrometheusResponse, error) {
	exampleFiles, err := recursivelyFindFilesWithSuffix(examplesDir, ".json")
	if err != nil {
		return nil, err
	}

	examplePayloads := make([]querymiddleware.PrometheusResponse, len(exampleFiles))
	originalJsonCodec := encoding.OriginalJsonCodec{}

	for i, file := range exampleFiles {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("could not read %v: %w", file, err)
		}

		payload, err := originalJsonCodec.Decode(b)
		if err != nil {
			return nil, fmt.Errorf("could not decode JSON file %v: %w", file, err)
		}

		examplePayloads[i] = payload
	}

	return examplePayloads, nil
}

func recursivelyFindFilesWithSuffix(dir string, suffix string) ([]string, error) {
	files := make([]string, 0)

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, suffix) {
			return nil
		}

		files = append(files, path)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

type codecStats struct {
	minSize   int
	maxSize   int
	avgSize   float64
	totalSize int
}

func calculateStatsForCodec(codec encoding.Codec, examplePayloads []querymiddleware.PrometheusResponse) (codecStats, error) {
	stats := codecStats{
		minSize:   math.MaxInt,
		maxSize:   -1,
		totalSize: 0,
	}

	for _, examplePayload := range examplePayloads {
		b, err := codec.Encode(examplePayload)
		if err != nil {
			return codecStats{}, err
		}

		size := len(b)
		stats.minSize = intmath.Min(stats.minSize, size)
		stats.maxSize = intmath.Max(stats.maxSize, size)
		stats.totalSize += size
	}

	stats.avgSize = float64(stats.totalSize) / float64(len(examplePayloads))

	return stats, nil
}
