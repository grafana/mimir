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

const (
	outputDir           = "chunks-dump"
	portForwardWaitTime = 5 * time.Second
	goroutineStartDelay = 250 * time.Millisecond
	minPort             = 16384
	maxPortRange        = 49152 - 16384
	defaultTimeout      = 30 * time.Second
	ingesterPort        = 9095
)

func main() {
	app := kingpin.New("grpcurl-query-ingesters", "Tool to download chunks from ingesters and dump their content")

	// Download chunks command
	downloadCmd := app.Command("download-chunks", "Download chunks from Kubernetes ingesters")
	k8sContext := downloadCmd.Arg("context", "Kubernetes context").Required().String()
	k8sNamespace := downloadCmd.Arg("namespace", "Kubernetes namespace").Required().String()
	tenantID := downloadCmd.Arg("tenant-id", "Mimir tenant ID").Required().String()
	queryFile := downloadCmd.Arg("query-file", "JSON query file").Required().String()
	downloadCmd.Action(func(c *kingpin.ParseContext) error {
		downloadChunks(*k8sContext, *k8sNamespace, *tenantID, *queryFile)
		return nil
	})

	// Dump command (default command when no subcommand specified)
	dumpCmd := app.Command("dump", "Dump content of downloaded chunk files").Default()
	dumpFiles := dumpCmd.Arg("files", "Chunk dump files to parse").Required().Strings()
	dumpCmd.Action(func(c *kingpin.ParseContext) error {
		for _, file := range *dumpFiles {
			resps, err := parseFile(file)
			if err != nil {
				fmt.Printf("Failed to parse file: %v\n", err)
				os.Exit(1)
			}

			for _, res := range resps {
				dumpResponse(res)
			}
		}
		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))
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
	if err := setupOutputDirectory(); err != nil {
		fmt.Printf("Failed to setup output directory: %v\n", err)
		os.Exit(1)
	}

	pods, err := getIngesterPods(k8sContext, k8sNamespace)
	if err != nil {
		fmt.Printf("Failed to get ingester pods: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d ingester pods\n", len(pods))

	failures := queryAllIngesters(k8sContext, k8sNamespace, tenantID, queryFile, pods)
	if hasFailures := handleResults(failures); hasFailures {
		os.Exit(1)
	}
}

func setupOutputDirectory() error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	failuresFile := filepath.Join(outputDir, ".failures")
	if err := os.WriteFile(failuresFile, []byte{}, 0644); err != nil {
		return fmt.Errorf("failed to create failures file: %w", err)
	}

	return nil
}

func queryAllIngesters(k8sContext, k8sNamespace, tenantID, queryFile string, pods []string) []string {
	basePort := minPort + rand.Intn(maxPortRange)
	var wg sync.WaitGroup
	var failures []string
	failuresMutex := &sync.Mutex{}

	for i, pod := range pods {
		localPort := basePort + i
		wg.Add(1)

		go func(podName string, port int) {
			defer wg.Done()

			err := queryIngester(k8sContext, k8sNamespace, podName, tenantID, queryFile, port)
			if err != nil {
				printFailure(fmt.Sprintf("Failed to query %s", podName))

				failuresMutex.Lock()
				failures = append(failures, podName)
				failuresMutex.Unlock()
			} else {
				printSuccess(fmt.Sprintf("Successfully queried %s", podName))
			}
		}(pod, localPort)

		time.Sleep(goroutineStartDelay)
	}

	wg.Wait()
	return failures
}

func handleResults(failures []string) bool {
	// Write failures file for backward compatibility
	failuresFile := filepath.Join(outputDir, ".failures")
	var failureData []byte
	if len(failures) > 0 {
		failureData = []byte(strings.Join(failures, "\n") + "\n")
	}
	os.WriteFile(failuresFile, failureData, 0644)

	fmt.Println()
	fmt.Println()

	if len(failures) == 0 {
		printSuccess("Successfully queried all ingesters")
		return false
	} else {
		printFailure(fmt.Sprintf("Failed to query %d ingesters:", len(failures)))
		for _, failure := range failures {
			fmt.Printf("- %s\n", failure)
		}
		return true
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

func queryIngester(k8sContext, k8sNamespace, podName, tenantID, queryFile string, localPort int) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	portForwardCmd := exec.CommandContext(ctx, "kubectl", "port-forward",
		"--context", k8sContext, "-n", k8sNamespace, podName,
		fmt.Sprintf("%d:%d", localPort, ingesterPort))

	err := portForwardCmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start port-forward: %w", err)
	}

	defer func() {
		if portForwardCmd.Process != nil {
			portForwardCmd.Process.Kill()
			if waitErr := portForwardCmd.Wait(); waitErr != nil {
				// Log but don't fail on cleanup errors
				fmt.Fprintf(os.Stderr, "Warning: error waiting for port-forward cleanup: %v\n", waitErr)
			}
		}
	}()

	time.Sleep(portForwardWaitTime)

	queryData, err := os.ReadFile(queryFile)
	if err != nil {
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

	if err != nil {
		return fmt.Errorf("grpcurl query failed for pod %s on port %d: %w", podName, localPort, err)
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
