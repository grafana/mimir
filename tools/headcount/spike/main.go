// SPDX-License-Identifier: AGPL-3.0-only

// Command spike writes a handful of block-builder-shaped level-1 blocks into a
// filesystem bucket, to check that the native compactor accepts and splits them.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func main() {
	bucket := flag.String("bucket", "", "filesystem bucket directory")
	series := flag.Int("series", 1000, "series per partition")
	partitions := flag.Int("partitions", 2, "partitions")
	hours := flag.Int("hours", 4, "hours")
	flag.Parse()

	userDir := filepath.Join(*bucket, "anonymous")
	if err := os.MkdirAll(userDir, 0o755); err != nil {
		log.Fatal(err)
	}
	t0 := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	const hour = int64(time.Hour / time.Millisecond)

	for h := 0; h < *hours; h++ {
		for p := 0; p < *partitions; p++ {
			var specs block.SeriesSpecs
			for i := 0; i < *series; i++ {
				lset := labels.FromStrings("__name__", fmt.Sprintf("metric_%03d", i%50), "instance", fmt.Sprintf("i-%d", i), "partition", fmt.Sprint(p))
				minT := t0 + int64(h)*hour
				maxT := minT + hour - 15000
				specs = append(specs, &block.SeriesSpec{Labels: lset, Chunks: []chunks.Meta{twoSampleChunk(minT, maxT)}})
			}
			meta, err := block.GenerateBlockFromSpec(userDir, specs)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(meta.ULID, "partition", p, "hour", h, "series", meta.Stats.NumSeries)
		}
	}
}

func twoSampleChunk(minT, maxT int64) chunks.Meta {
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		log.Fatal(err)
	}
	app.Append(0, minT, 1)
	app.Append(0, maxT, 1)
	return chunks.Meta{Chunk: c, MinTime: minT, MaxTime: maxT}
}
