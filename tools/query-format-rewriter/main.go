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
	"golang.org/x/exp/slices"
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

	internedJsonDir := path.Join(workingDir, "interned-json")

	if err := os.RemoveAll(internedJsonDir); err != nil {
		fmt.Printf("Error cleaning output directory: %v\n", err)
		os.Exit(1)
	}

	if err := os.Mkdir(internedJsonDir, 0700); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	for _, originalFormatFile := range originalFormatFiles {
		originalFormat, err := parseOriginalFormat(originalFormatFile)

		if err != nil {
			fmt.Printf("Error reading %v: %v\n", originalFormatFile, err)
			os.Exit(1)
		}

		internedStringsFormat, err := convertResponseToInternedStringsFormat(originalFormat)

		if err != nil {
			fmt.Printf("Error converting %v: %v\n", originalFormatFile, err)
			os.Exit(1)
		}

		relativeName, err := filepath.Rel(originalFormatDir, originalFormatFile)

		if err != nil {
			fmt.Printf("Error determining relative path: %v\n", err)
			os.Exit(1)
		}

		internedStringsFile := path.Join(internedJsonDir, relativeName)
		parentDir := path.Dir(internedStringsFile)

		if err := os.MkdirAll(parentDir, 0700); err != nil {
			fmt.Printf("Error creating output directory: %v\n", err)
			os.Exit(1)
		}

		jsonBytes, err := json.Marshal(internedStringsFormat)

		if err != nil {
			fmt.Printf("Error marshalling to JSON: %v\n", err)
			os.Exit(1)
		}

		if err := os.WriteFile(internedStringsFile, jsonBytes, 0700); err != nil {
			fmt.Printf("Error writing file %v: %v\n", internedStringsFile, err)
			os.Exit(1)
		}
	}

	// TODO: emit protobuf format as well
	// TODO: emit statistics (before and after file sizes, number of unique strings, total number of strings)
}

func parseOriginalFormat(f string) (originalFormatAPIResponse, error) {
	b, err := os.ReadFile(f)

	if err != nil {
		return originalFormatAPIResponse{}, err
	}

	resp := originalFormatAPIResponse{}

	if err := json.Unmarshal(b, &resp); err != nil {
		return originalFormatAPIResponse{}, err
	}

	return resp, nil
}

func convertResponseToInternedStringsFormat(o originalFormatAPIResponse) (internedStringsAPIResponse, error) {
	data, err := convertDataToInternedStringsFormat(o.Data)

	if err != nil {
		return internedStringsAPIResponse{}, nil
	}

	return internedStringsAPIResponse{
		Status:    o.Status,
		Data:      data,
		ErrorType: o.ErrorType,
		Error:     o.Error,
	}, nil
}

func convertDataToInternedStringsFormat(o originalFormatData) (internedStringsData, error) {
	switch o.Type {
	case model.ValVector:
		return convertVectorDataToInternedStringsFormat(o.Result)

	default:
		return internedStringsData{}, fmt.Errorf("unsupported value type %v", o.Type)
	}
}

// TODO: for protobuf, we might get a small payload size reduction if we use lower symbol ordinals for
// label names, as we'd expect these would be repeated more often. The lower ordinals would encode as fewer
// bytes if we use uvarints for encoding.
// Easiest way to achieve this would be to scan through all label names first, then scan through label values.
func convertVectorDataToInternedStringsFormat(raw json.RawMessage) (internedStringsData, error) {
	var originalVector model.Vector
	if err := json.Unmarshal(raw, &originalVector); err != nil {
		return internedStringsData{}, fmt.Errorf("could not decode vector result: %w", err)
	}

	invertedSymbols := map[string]internedSymbolRef{}
	convertedVector := make(internedStringVector, 0, len(originalVector))

	for _, originalSample := range originalVector {
		convertedMetric := make(internedStringMetric, len(originalSample.Metric))

		for n, v := range originalSample.Metric {
			if _, ok := invertedSymbols[string(n)]; !ok {
				invertedSymbols[string(n)] = internedSymbolRef(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[string(v)]; !ok {
				invertedSymbols[string(v)] = internedSymbolRef(len(invertedSymbols))
			}

			convertedMetric[invertedSymbols[string(n)]] = invertedSymbols[string(v)]
		}

		convertedSample := internedStringSample{
			Value:     originalSample.Value,
			Timestamp: originalSample.Timestamp,
			Metric:    convertedMetric,
		}

		convertedVector = append(convertedVector, convertedSample)
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return internedStringsData{
		Type:    model.ValVector,
		Result:  convertedVector,
		Symbols: symbols,
	}, nil
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

type internedStringsAPIResponse struct {
	Status    string              `json:"status"`
	Data      internedStringsData `json:"data"`
	ErrorType string              `json:"errorType,omitempty"`
	Error     string              `json:"error,omitempty"`
}

type internedStringsData struct {
	Type    model.ValueType `json:"resultType"`
	Result  any             `json:"result"`
	Symbols []string        `json:"symbols"`
}

type internedStringVector []internedStringSample

type internedStringSample struct {
	Metric    internedStringMetric `json:"metric"`
	Value     model.SampleValue    `json:"value"`
	Timestamp model.Time           `json:"timestamp"`
}

// This is based on model.Sample - it encodes the value as an array to avoid including lots of instances of
// "value" and "timestamp" in the final JSON output.
func (s internedStringSample) MarshalJSON() ([]byte, error) {
	// Convert Metric to a slice to avoid encoding the label name ordinals as strings
	// (JSON requires that object keys be strings)
	metricAsSlice := make([]internedSymbolRef, 0, len(s.Metric)*2)

	for n, v := range s.Metric {
		metricAsSlice = append(metricAsSlice, n, v)
	}

	v := struct {
		Metric []internedSymbolRef `json:"metric"`
		Value  model.SamplePair    `json:"value"`
	}{
		Metric: metricAsSlice,
		Value: model.SamplePair{
			Timestamp: s.Timestamp,
			Value:     s.Value,
		},
	}

	return json.Marshal(&v)
}

type internedStringMetric map[internedSymbolRef]internedSymbolRef

type internedSymbolRef uint
