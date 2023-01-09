// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/querier/internedquerypb"
)

func main() {
	workingDir := ""
	flag.StringVar(&workingDir, "working-dir", "", "Directory containing files in original JSON query format in a subdirectory named 'original-format'. Rewritten files will written to other subdirectories.")
	flag.Parse()

	if workingDir == "" {
		fmt.Println("Missing required command line flag: working directory")
		os.Exit(1)
	}

	originalFormatDir := path.Join(workingDir, "original-format")
	originalFormatFiles, err := filepath.Glob(path.Join(originalFormatDir, "**", "*.json"))

	if err != nil {
		fmt.Printf("Error listing original format files: %v\n", err)
		os.Exit(1)
	}

	slices.Sort(originalFormatFiles)

	internedProtobufDir := path.Join(workingDir, "interned-protobuf")

	for _, d := range []string{internedProtobufDir} {
		if err := ensureCreatedAndClean(d); err != nil {
			fmt.Printf("Could not create output directory %v: %v\n", d, err)
			os.Exit(1)
		}
	}

	fmt.Println("Name\tOriginal number of strings\tUnique strings\tOriginal size (bytes)\tInterned Protobuf format size (bytes)")

	for _, originalFormatFile := range originalFormatFiles {
		originalFormat, originalFormatSize, err := parseOriginalFormat(originalFormatFile)

		if err != nil {
			fmt.Printf("Error reading %v: %v\n", originalFormatFile, err)
			os.Exit(1)
		}

		internedProtobufBytes, originalStringCount, uniqueStringCount, err := convertToProtobufResponse(originalFormat)

		if err != nil {
			fmt.Printf("Error converting to protobuf: %v\n", err)
			os.Exit(1)
		}

		relativeName, err := filepath.Rel(originalFormatDir, originalFormatFile)

		if err != nil {
			fmt.Printf("Error determining relative path: %v\n", err)
			os.Exit(1)
		}

		internedProtobufFile := path.Join(internedProtobufDir, relativeName+".pb")

		if err := createParentAndWriteFile(internedProtobufFile, internedProtobufBytes); err != nil {
			fmt.Printf("Error writing file %v: %v\n", internedProtobufFile, err)
			os.Exit(1)
		}

		fmt.Printf("%v\t%v\t%v\t%v\t%v\n", relativeName, originalStringCount, uniqueStringCount, originalFormatSize, len(internedProtobufBytes))
	}
}

func ensureCreatedAndClean(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("cleaning directory: %w", err)
	}

	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("creating directory: %w", err)
	}

	return nil
}

func createParentAndWriteFile(file string, data []byte) error {
	parentDir := path.Dir(file)

	if err := os.MkdirAll(parentDir, 0700); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	if err := os.WriteFile(file, data, 0700); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	return nil
}

func parseOriginalFormat(f string) (originalFormatAPIResponse, int, error) {
	b, err := os.ReadFile(f)

	if err != nil {
		return originalFormatAPIResponse{}, 0, err
	}

	resp := originalFormatAPIResponse{}

	if err := json.Unmarshal(b, &resp); err != nil {
		return originalFormatAPIResponse{}, 0, err
	}

	return resp, len(b), nil
}

type originalFormatAPIResponse struct {
	Status    string             `json:"status"`
	Data      originalFormatData `json:"data"`
	ErrorType string             `json:"errorType"`
	Error     string             `json:"error"`
}

type originalFormatData struct {
	Type   model.ValueType `json:"resultType"`
	Result json.RawMessage `json:"result"`
}

func convertToProtobufResponse(r originalFormatAPIResponse) (b []byte, originalStringCount int, uniqueStringCount int, err error) {
	resp := internedquerypb.QueryResponse{
		Status:    r.Status,
		ErrorType: r.ErrorType,
		Error:     r.Error,
	}

	switch r.Data.Type {
	case model.ValVector:
		data, originalStringCount, uniqueStringCount, err := convertToProtobufVector(r.Data)

		if err != nil {
			return nil, 0, 0, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, 0, 0, err
		}

		return b, originalStringCount, uniqueStringCount, nil

	case model.ValScalar:
		data, err := convertToProtobufScalar(r.Data)

		if err != nil {
			return nil, 0, 0, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, 0, 0, err
		}

		return b, 0, 0, nil

	default:
		panic(fmt.Sprintf("unsupported data type: %v", r.Data.Type))
	}
}

func convertToProtobufVector(original originalFormatData) (v *internedquerypb.QueryResponse_Vector, originalStringCount int, uniqueStringCount int, err error) {
	var originalVector model.Vector
	if err := json.Unmarshal(original.Result, &originalVector); err != nil {
		return nil, 0, 0, fmt.Errorf("could not decode vector result: %w", err)
	}

	invertedSymbols := map[string]uint64{}
	samples := make([]*internedquerypb.Sample, len(originalVector))
	originalStringCount = 0

	for i, originalSample := range originalVector {
		// This is somewhat convoluted: we do this to ensure we emit the labels in a stable order.
		// (labels.Builder.Labels() sorts the labels before returning the built label set.)
		lb := labels.NewBuilder(labels.EmptyLabels())
		for n, v := range originalSample.Metric {
			lb.Set(string(n), string(v))
		}

		metricSymbols := make([]uint64, 0, len(originalSample.Metric)*2)

		for _, l := range lb.Labels(nil) {
			if _, ok := invertedSymbols[l.Name]; !ok {
				invertedSymbols[l.Name] = uint64(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[l.Value]; !ok {
				invertedSymbols[l.Value] = uint64(len(invertedSymbols))
			}

			metricSymbols = append(metricSymbols, invertedSymbols[l.Name], invertedSymbols[l.Value])
			originalStringCount += 2
		}

		samples[i] = &internedquerypb.Sample{
			Value:         float64(originalSample.Value),
			Timestamp:     int64(originalSample.Timestamp),
			MetricSymbols: metricSymbols,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	uniqueStringCount = len(symbols)

	return &internedquerypb.QueryResponse_Vector{
		Vector: &internedquerypb.VectorData{
			Symbols: symbols,
			Samples: samples,
		},
	}, originalStringCount, uniqueStringCount, nil
}

func convertToProtobufScalar(d originalFormatData) (*internedquerypb.QueryResponse_Scalar, error) {
	var originalScalar model.Scalar
	if err := json.Unmarshal(d.Result, &originalScalar); err != nil {
		return nil, fmt.Errorf("could not decode scalar result: %w", err)
	}

	return &internedquerypb.QueryResponse_Scalar{
		Scalar: &internedquerypb.ScalarData{
			Timestamp: int64(originalScalar.Timestamp),
			Value:     float64(originalScalar.Value),
		},
	}, nil
}
