// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "download-chunks" {
		app := kingpin.New("grpcurl-query-ingesters", "Download chunks from Kubernetes ingesters")
		k8sContext := app.Arg("context", "Kubernetes context").Required().String()
		k8sNamespace := app.Arg("namespace", "Kubernetes namespace").Required().String()
		tenantID := app.Arg("tenant-id", "Mimir tenant ID").Required().String()
		queryFile := app.Arg("query-file", "JSON query file").Required().String()

		kingpin.MustParse(app.Parse(os.Args[2:]))
		downloadChunks(*k8sContext, *k8sNamespace, *tenantID, *queryFile)
		return
	}

	if len(os.Args) == 1 {
		fmt.Println("Error: no files specified")
		fmt.Println("Usage:")
		fmt.Println("  go run . <dump-files>...                              # Dump content of chunk files")
		fmt.Println("  go run . download-chunks <context> <namespace> <tenant> <query-file>  # Download chunks")
		fmt.Println()
		fmt.Println("For more help:")
		fmt.Println("  go run . download-chunks --help")
		os.Exit(1)
	}

	for _, file := range os.Args[1:] {
		resps, err := parseFile(file)
		if err != nil {
			fmt.Println("Failed to parse file:", err.Error())
			os.Exit(1)
		}

		for _, res := range resps {
			dumpResponse(res)
		}
	}
}

func parseFile(file string) ([]QueryStreamResponse, error) {
	resps := []QueryStreamResponse{}

	fileData, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	// Decode file.
	decoder := json.NewDecoder(bytes.NewReader(fileData))
	for {
		res := QueryStreamResponse{}
		if err := decoder.Decode(&res); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		resps = append(resps, res)
	}

	return resps, nil
}

func dumpResponse(res QueryStreamResponse) {
	slices.SortFunc(res.Chunkseries, func(a, b QueryStreamChunkseries) int {
		return labels.Compare(a.LabelSet(), b.LabelSet())
	})

	for _, series := range res.Chunkseries {
		fmt.Println(series.LabelSet().String())
		var (
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ts int64
		)

		slices.SortFunc(series.Chunks, func(a, b Chunk) int {
			return int(a.StartTimestamp() - b.StartTimestamp())
		})

		for _, chunk := range series.Chunks {
			fmt.Printf(
				"- Chunk: %s - %s\n",
				chunk.StartTime().Format(time.RFC3339),
				chunk.EndTime().Format(time.RFC3339))

			chunkIterator := chunk.EncodedChunk().NewIterator(nil)
			for {
				sampleType := chunkIterator.Scan()
				if sampleType == chunkenc.ValNone {
					break
				}

				switch sampleType {
				case chunkenc.ValFloat:
					fmt.Println("  - Sample:", sampleType.String(), "ts:", chunkIterator.Timestamp(), "value:", chunkIterator.Value().Value)
				case chunkenc.ValHistogram:
					ts, h = chunkIterator.AtHistogram(h)
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", h, "hint:", counterResetHintString(h.CounterResetHint))
				case chunkenc.ValFloatHistogram:
					ts, fh := chunkIterator.AtFloatHistogram(fh)
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", fh, "hint:", counterResetHintString(fh.CounterResetHint))
				default:
					panic(fmt.Errorf("unknown sample type %s", sampleType.String()))
				}
			}

			if chunkIterator.Err() != nil {
				panic(chunkIterator.Err())
			}
		}
	}
}

func counterResetHintString(crh histogram.CounterResetHint) string {
	switch crh {
	case histogram.UnknownCounterReset:
		return "UnknownCounterReset"
	case histogram.CounterReset:
		return "CounterReset"
	case histogram.NotCounterReset:
		return "NotCounterReset"
	case histogram.GaugeType:
		return "GaugeType"
	default:
		return "unrecognized counter reset hint"
	}
}

func downloadChunks(k8sContext, k8sNamespace, tenantID, queryFile string) {

	outputDir := "chunks-dump"
	failuresFile := filepath.Join(outputDir, ".failures")

	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	err = os.WriteFile(failuresFile, []byte{}, 0644)
	if err != nil {
		fmt.Printf("Failed to create failures file: %v\n", err)
		os.Exit(1)
	}

	pods, err := getIngesterPods(k8sContext, k8sNamespace)
	if err != nil {
		fmt.Printf("Failed to get ingester pods: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d ingester pods\n", len(pods))

	basePort := 16384 + rand.Intn(49152-16384)
	var wg sync.WaitGroup
	failuresMutex := &sync.Mutex{}

	for i, pod := range pods {
		localPort := basePort + i
		wg.Add(1)

		go func(podName string, port int) {
			defer wg.Done()

			err := queryIngester(k8sContext, k8sNamespace, podName, tenantID, queryFile, outputDir, port)
			if err != nil {
				printFailure(fmt.Sprintf("Failed to query %s", podName))

				failuresMutex.Lock()
				f, _ := os.OpenFile(failuresFile, os.O_APPEND|os.O_WRONLY, 0644)
				if f != nil {
					f.WriteString(podName + "\n")
					f.Close()
				}
				failuresMutex.Unlock()
			} else {
				printSuccess(fmt.Sprintf("Successfully queried %s", podName))
			}
		}(pod, localPort)

		time.Sleep(250 * time.Millisecond)
	}

	wg.Wait()

	failuresData, _ := os.ReadFile(failuresFile)
	failuresList := strings.TrimSpace(string(failuresData))

	fmt.Println()
	fmt.Println()

	if failuresList == "" {
		printSuccess("Successfully queried all ingesters")
	} else {
		failures := strings.Split(failuresList, "\n")
		printFailure(fmt.Sprintf("Failed to query %d ingesters:", len(failures)))

		for _, failure := range failures {
			if failure != "" {
				fmt.Printf("- %s\n", failure)
			}
		}
		os.Exit(1)
	}
}

func getIngesterPods(k8sContext, k8sNamespace string) ([]string, error) {
	cmd := exec.Command("kubectl", "--context", k8sContext, "-n", k8sNamespace,
		"get", "pods", "--no-headers", "-o", "custom-columns=NAME:.metadata.name")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get pods: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var ingesterPods []string

	for _, line := range lines {
		if strings.Contains(line, "ingester") {
			ingesterPods = append(ingesterPods, line)
		}
	}

	return ingesterPods, nil
}

func queryIngester(k8sContext, k8sNamespace, podName, tenantID, queryFile, outputDir string, localPort int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	portForwardCmd := exec.CommandContext(ctx, "kubectl", "port-forward",
		"--context", k8sContext, "-n", k8sNamespace, podName,
		fmt.Sprintf("%d:9095", localPort))

	err := portForwardCmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start port-forward: %w", err)
	}

	time.Sleep(5 * time.Second)

	queryData, err := os.ReadFile(queryFile)
	if err != nil {
		portForwardCmd.Process.Kill()
		return fmt.Errorf("failed to read query file: %w", err)
	}

	grpcurlCmd := exec.CommandContext(ctx, "grpcurl",
		"-d", string(queryData),
		"-H", fmt.Sprintf("X-Scope-OrgID: %s", tenantID),
		"-proto", "pkg/ingester/client/ingester.proto",
		"-import-path", "../..",
		"-import-path", "../../vendor",
		"-plaintext",
		fmt.Sprintf("localhost:%d", localPort),
		"cortex.Ingester/QueryStream")

	outputFile := filepath.Join(outputDir, podName)
	output, err := grpcurlCmd.Output()

	portForwardCmd.Process.Kill()
	portForwardCmd.Wait()

	if err != nil {
		return fmt.Errorf("grpcurl failed: %w", err)
	}

	err = os.WriteFile(outputFile, output, 0644)
	if err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}

func printSuccess(message string) {
	fmt.Printf("\033[0;32m%s\033[0m\n", message)
}

func printFailure(message string) {
	fmt.Printf("\033[0;31m%s\033[0m\n", message)
}
