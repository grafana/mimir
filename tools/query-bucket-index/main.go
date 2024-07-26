// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

// Invoke this with a command like:
//
//	go run . -bucket-index bucket-index.json.gz -start 2024-07-24T12:00:00Z -end 2024-07-24T13:00:00Z
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	bucketIndexFile, bucketIndexIsCompressed, startT, endT, err := parseFlags()
	if err != nil {
		return err
	}

	bucketIndex, err := readBucketIndex(bucketIndexFile, bucketIndexIsCompressed)
	if err != nil {
		return err
	}

	blocks, deletionMarkers, err := getBlocks(bucketIndex, timestamp.FromTime(startT), timestamp.FromTime(endT))
	if err != nil {
		return err
	}

	slices.SortFunc(blocks, func(a, b *bucketindex.Block) int {
		if a.ID.String() > b.ID.String() {
			return 1
		} else if a.ID.String() < b.ID.String() {
			return -1
		}

		return 0
	})

	w := csv.NewWriter(os.Stdout)
	err = w.Write([]string{
		"Block ID",
		"Min time",
		"Max time",
		"Uploaded at",
		"Shard ID",
		"Source",
		"Compaction level",
		"Deletion marker present?",
		"Deletion time",
	})
	if err != nil {
		return err
	}

	for _, block := range blocks {
		deletionMarker, deletionMarkerPresent := deletionMarkers[block.ID]
		deletionTime := ""

		if deletionMarkerPresent {
			deletionTime = time.Unix(deletionMarker.DeletionTime, 0).UTC().Format(time.RFC3339)
		}

		err := w.Write([]string{
			block.ID.String(),
			timestamp.Time(block.MinTime).Format(time.RFC3339),
			timestamp.Time(block.MaxTime).Format(time.RFC3339),
			time.Unix(block.UploadedAt, 0).UTC().Format(time.RFC3339),
			block.CompactorShardID,
			block.Source,
			strconv.Itoa(block.CompactionLevel),
			strconv.FormatBool(deletionMarkerPresent),
			deletionTime,
		})
		if err != nil {
			return err
		}
	}

	w.Flush()

	return w.Error()
}

func parseFlags() (string, bool, time.Time, time.Time, error) {
	bucketIndexFile := flag.String("bucket-index", "", "Path to bucket index file (uncompressed JSON)")
	compressed := flag.Bool("bucket-index-compressed", true, "If true, treat the file provided with -bucket-index as a gzip-compressed JSON file. If false, treat the file as an uncompressed JSON file.")

	var startT, endT flagext.Time
	flag.Var(&startT, "start", "Start time")
	flag.Var(&endT, "end", "End time")

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		return "", false, time.Time{}, time.Time{}, err
	}

	if *bucketIndexFile == "" {
		return "", false, time.Time{}, time.Time{}, errors.New("must provide bucket index file")
	}

	if time.Time(startT).IsZero() {
		return "", false, time.Time{}, time.Time{}, errors.New("must provide start time")
	}

	if time.Time(endT).IsZero() {
		return "", false, time.Time{}, time.Time{}, errors.New("must provide start time")
	}

	return *bucketIndexFile, *compressed, time.Time(startT), time.Time(endT), nil
}

func readBucketIndex(bucketIndexFile string, isCompressed bool) (*bucketindex.Index, error) {
	f, err := os.Open(bucketIndexFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var reader io.Reader

	if isCompressed {
		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}

		defer gzipReader.Close()

		reader = gzipReader
	} else {
		reader = f
	}

	index := &bucketindex.Index{}
	d := json.NewDecoder(reader)
	if err := d.Decode(index); err != nil {
		return nil, err
	}

	return index, nil
}

// This is based on BucketIndexBlocksFinder.GetBlocks.
func getBlocks(index *bucketindex.Index, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	matchingBlocks := map[ulid.ULID]*bucketindex.Block{}
	matchingDeletionMarks := map[ulid.ULID]*bucketindex.BlockDeletionMark{}

	for _, block := range index.Blocks {
		if !block.Within(minT, maxT) {
			continue
		}

		matchingBlocks[block.ID] = block
	}

	for _, mark := range index.BlockDeletionMarks {
		// Filter deletion marks by matching blocks only.
		if _, ok := matchingBlocks[mark.ID]; !ok {
			continue
		}

		matchingDeletionMarks[mark.ID] = mark
	}

	// Convert matching blocks into a list.
	blocks := make(bucketindex.Blocks, 0, len(matchingBlocks))
	for _, b := range matchingBlocks {
		blocks = append(blocks, b)
	}

	return blocks, matchingDeletionMarks, nil
}
