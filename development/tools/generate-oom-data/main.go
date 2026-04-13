// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	var (
		outputDir      string
		numSeries      int
		daysBack       int
		blockDurationH int
	)

	flag.StringVar(&outputDir, "output-dir", "", "Directory to write TSDB blocks to (required)")
	flag.IntVar(&numSeries, "num-series", 3000, "Number of kube_node_info series to generate")
	flag.IntVar(&daysBack, "days-back", 15, "Number of days of historical data to generate")
	flag.IntVar(&blockDurationH, "block-duration-hours", 2, "Duration of each TSDB block in hours")
	flag.Parse()

	if outputDir == "" {
		log.Fatal("-output-dir is required")
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	seriesLabels := generateSeriesLabels(numSeries)
	log.Printf("Generated %d series label sets", len(seriesLabels))

	blockDuration := time.Duration(blockDurationH) * time.Hour
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-time.Duration(daysBack) * 24 * time.Hour)

	totalBlocks := int(endTime.Sub(startTime) / blockDuration)
	log.Printf("Generating %d blocks from %s to %s (block duration: %s)", totalBlocks, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), blockDuration)

	for blockStart := startTime; blockStart.Before(endTime); blockStart = blockStart.Add(blockDuration) {
		blockEnd := blockStart.Add(blockDuration)
		if blockEnd.After(endTime) {
			blockEnd = endTime
		}

		blockID, err := generateBlock(outputDir, seriesLabels, blockStart, blockEnd, blockDuration)
		if err != nil {
			log.Fatalf("generate block [%s, %s): %v", blockStart.Format(time.RFC3339), blockEnd.Format(time.RFC3339), err)
		}

		remaining := int(endTime.Sub(blockEnd) / blockDuration)
		log.Printf("Created block %s [%s, %s) — %d blocks remaining",
			blockID, blockStart.Format("Jan02 15:04"), blockEnd.Format("Jan02 15:04"), remaining)
	}

	log.Printf("Done. Blocks written to %s", outputDir)
}

func generateBlock(outputDir string, seriesLabels []labels.Labels, blockStart, blockEnd time.Time, blockDuration time.Duration) (string, error) {
	blockDurMs := blockDuration.Milliseconds()
	// BlockWriter needs blockSize >= the range of samples. Use 2x for safety.
	w, err := tsdb.NewBlockWriter(promslog.NewNopLogger(), outputDir, blockDurMs*2)
	if err != nil {
		return "", fmt.Errorf("new block writer: %w", err)
	}
	defer w.Close()

	ctx := context.Background()
	app := w.Appender(ctx)

	sampleInterval := time.Minute
	for t := blockStart; t.Before(blockEnd); t = t.Add(sampleInterval) {
		ts := t.UnixMilli()
		for i, lbls := range seriesLabels {
			if _, err := app.Append(0, lbls, ts, 1.0); err != nil {
				return "", fmt.Errorf("append series %d at %d: %w", i, ts, err)
			}
		}
	}

	if err := app.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}

	blockID, err := w.Flush(ctx)
	if err != nil {
		return "", fmt.Errorf("flush: %w", err)
	}

	// Write no-compact marker so the compactor leaves these blocks alone.
	markerPath := filepath.Join(outputDir, blockID.String(), "no-compact-mark.json")
	marker := fmt.Sprintf(`{"id":"%s","reason":"manual","details":"generated for OOM reproduction","no_compact_time":0,"version":1}`, blockID.String())
	if err := os.WriteFile(markerPath, []byte(marker), 0o644); err != nil {
		return "", fmt.Errorf("write no-compact marker: %w", err)
	}

	return blockID.String(), nil
}

func generateSeriesLabels(numSeries int) []labels.Labels {
	kernelVersions := []string{
		"5.15.0-1054-aws", "5.15.0-1055-aws", "5.15.0-1056-aws",
		"6.1.0-1033-aws", "6.1.0-1034-aws",
	}
	kubeletVersions := []string{"v1.28.4", "v1.28.5", "v1.29.1"}
	osImages := []string{
		"Ubuntu 22.04.3 LTS", "Ubuntu 22.04.4 LTS", "Ubuntu 24.04 LTS",
	}
	containerRuntimes := []string{
		"containerd://1.7.11", "containerd://1.7.12", "containerd://1.7.13",
	}
	assertsEnvs := []string{"prod"}
	assertsSites := []string{"us-east-1", "eu-west-1"}

	result := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		b := labels.NewScratchBuilder(13)
		b.Add("__name__", "kube_node_info")
		b.Add("asserts_env", assertsEnvs[i%len(assertsEnvs)])
		b.Add("asserts_site", assertsSites[i%len(assertsSites)])
		b.Add("cluster", "orchestration-cluster-a7a4439")
		b.Add("container_runtime_version", containerRuntimes[i%len(containerRuntimes)])
		b.Add("internal_ip", fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256))
		b.Add("kernel_version", kernelVersions[i%len(kernelVersions)])
		b.Add("kubelet_version", kubeletVersions[i%len(kubeletVersions)])
		b.Add("node", fmt.Sprintf("ip-10-%d-%d-%d.ec2.internal", (i/65536)%256, (i/256)%256, i%256))
		b.Add("os_image", osImages[i%len(osImages)])
		b.Add("pod_cidr", fmt.Sprintf("100.%d.%d.0/24", (i/256)%256, i%256))
		b.Add("provider_id", fmt.Sprintf("aws:///us-east-1a/i-%016x", i))
		b.Add("system_uuid", fmt.Sprintf("ec2%05d-0000-0000-0000-%012x", i, i))
		b.Sort()
		result = append(result, b.Labels())
	}

	return result
}
