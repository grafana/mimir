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

	"github.com/grafana/mimir/pkg/querier/querypb"
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
	internedProtobufDir := path.Join(workingDir, "interned-protobuf")

	for _, d := range []string{internedJsonDir, internedProtobufDir} {
		if err := ensureCreatedAndClean(d); err != nil {
			fmt.Printf("Could not create output directory %v: %v\n", d, err)
			os.Exit(1)
		}
	}

	fmt.Println("Name\tOriginal number of strings\tUnique strings\tOriginal size (bytes)\tInterned JSON format size (bytes)\tInterned Protobuf format size (bytes)")

	for _, originalFormatFile := range originalFormatFiles {
		originalFormat, originalFormatSize, err := parseOriginalFormat(originalFormatFile)

		if err != nil {
			fmt.Printf("Error reading %v: %v\n", originalFormatFile, err)
			os.Exit(1)
		}

		internedStringsFormat, originalStringCount, uniqueStringCount, err := convertResponseToInternedStringsFormat(originalFormat)

		if err != nil {
			fmt.Printf("Error converting %v: %v\n", originalFormatFile, err)
			os.Exit(1)
		}

		internedJSONBytes, err := json.Marshal(internedStringsFormat)

		if err != nil {
			fmt.Printf("Error marshalling to JSON: %v\n", err)
			os.Exit(1)
		}

		internedProtobufBytes, err := convertToProtobufResponse(internedStringsFormat)

		if err != nil {
			fmt.Printf("Error converting to protobuf: %v\n", err)
			os.Exit(1)
		}

		relativeName, err := filepath.Rel(originalFormatDir, originalFormatFile)

		if err != nil {
			fmt.Printf("Error determining relative path: %v\n", err)
			os.Exit(1)
		}

		internedJSONFile := path.Join(internedJsonDir, relativeName)

		if err := createParentAndWriteFile(internedJSONFile, internedJSONBytes); err != nil {
			fmt.Printf("Error writing file %v: %v\n", internedJSONFile, err)
			os.Exit(1)
		}

		internedProtobufFile := path.Join(internedProtobufDir, relativeName+".pb")

		if err := createParentAndWriteFile(internedProtobufFile, internedProtobufBytes); err != nil {
			fmt.Printf("Error writing file %v: %v\n", internedProtobufFile, err)
			os.Exit(1)
		}

		fmt.Printf("%v\t%v\t%v\t%v\t%v\t%v\n", relativeName, originalStringCount, uniqueStringCount, originalFormatSize, len(internedJSONBytes), len(internedProtobufBytes))
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

func convertResponseToInternedStringsFormat(o originalFormatAPIResponse) (internedStringsAPIResponse, int, int, error) {
	data, originalStringCount, uniqueStringCount, err := convertDataToInternedStringsFormat(o.Data)

	if err != nil {
		return internedStringsAPIResponse{}, 0, 0, err
	}

	return internedStringsAPIResponse{
		Status:    o.Status,
		Data:      data,
		ErrorType: o.ErrorType,
		Error:     o.Error,
	}, originalStringCount, uniqueStringCount, nil
}

func convertDataToInternedStringsFormat(o originalFormatData) (internedStringsData, int, int, error) {
	switch o.Type {
	case model.ValScalar:
		return convertScalarDataToInternedStringsFormat(o.Result)

	case model.ValVector:
		return convertVectorDataToInternedStringsFormat(o.Result)

	default:
		return internedStringsData{}, 0, 0, fmt.Errorf("unsupported value type %v", o.Type)
	}
}

func convertScalarDataToInternedStringsFormat(raw json.RawMessage) (internedStringsData, int, int, error) {
	// Scalars have no labels, so there is no conversion required.

	return internedStringsData{
		Type:   model.ValScalar,
		Result: raw,
	}, 0, 0, nil
}

// TODO: for protobuf, we might get a small payload size reduction if we use lower symbol ordinals for
// label names, as we'd expect these would be repeated more often. The lower ordinals would encode as fewer
// bytes if we use uvarints for encoding.
// Easiest way to achieve this would be to scan through all label names first, then scan through label values.
func convertVectorDataToInternedStringsFormat(raw json.RawMessage) (internedStringsData, int, int, error) {
	var originalVector model.Vector
	if err := json.Unmarshal(raw, &originalVector); err != nil {
		return internedStringsData{}, 0, 0, fmt.Errorf("could not decode vector result: %w", err)
	}

	invertedSymbols := map[string]internedSymbolRef{}
	convertedVector := make(internedStringVector, 0, len(originalVector))
	originalStringCount := 0

	for _, originalSample := range originalVector {
		// This is somewhat convoluted: we do this to ensure we emit the labels in a stable order.
		// (labels.Builder.Labels() sorts the labels before returning the built label set.)
		lb := labels.NewBuilder(labels.EmptyLabels())
		for n, v := range originalSample.Metric {
			lb.Set(string(n), string(v))
		}

		metricSymbols := make(internedSymbolPairs, 0, len(originalSample.Metric)*2)

		for _, l := range lb.Labels(nil) {
			if _, ok := invertedSymbols[l.Name]; !ok {
				invertedSymbols[l.Name] = internedSymbolRef(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[l.Value]; !ok {
				invertedSymbols[l.Value] = internedSymbolRef(len(invertedSymbols))
			}

			metricSymbols = append(metricSymbols, invertedSymbols[l.Name], invertedSymbols[l.Value])
			originalStringCount += 2
		}

		convertedSample := internedStringSample{
			Value:         originalSample.Value,
			Timestamp:     originalSample.Timestamp,
			MetricSymbols: metricSymbols,
		}

		convertedVector = append(convertedVector, convertedSample)
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	uniqueStringCount := len(symbols)

	return internedStringsData{
		Type:    model.ValVector,
		Result:  convertedVector,
		Symbols: symbols,
	}, originalStringCount, uniqueStringCount, nil
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
	Symbols []string        `json:"symbols,omitempty"`
}

type internedStringVector []internedStringSample

type internedStringSample struct {
	MetricSymbols internedSymbolPairs `json:"metric"`
	Value         model.SampleValue   `json:"value"`
	Timestamp     model.Time          `json:"timestamp"`
}

// This is based on model.Sample - it encodes the value as an array to avoid including lots of instances of
// "value" and "timestamp" in the final JSON output.
func (s internedStringSample) MarshalJSON() ([]byte, error) {
	v := struct {
		MetricSymbols []internedSymbolRef `json:"metric"`
		Value         model.SamplePair    `json:"value"`
	}{
		MetricSymbols: s.MetricSymbols,
		Value: model.SamplePair{
			Timestamp: s.Timestamp,
			Value:     s.Value,
		},
	}

	return json.Marshal(&v)
}

// Ordered label name-value pair references.
type internedSymbolPairs []internedSymbolRef

type internedSymbolRef uint64

func convertToProtobufResponse(r internedStringsAPIResponse) ([]byte, error) {
	resp := querypb.QueryResponse{
		Status:    r.Status,
		ErrorType: r.ErrorType,
		Error:     r.Error,
	}

	switch r.Data.Type {
	case model.ValVector:
		resp.Data = convertToProtobufVector(r.Data)

	case model.ValScalar:
		data, err := convertToProtobufScalar(r.Data)

		if err != nil {
			return nil, err
		}

		resp.Data = data

	default:
		panic(fmt.Sprintf("unsupported data type: %v", r.Data.Type))
	}

	return resp.Marshal()
}

func convertToProtobufVector(d internedStringsData) *querypb.QueryResponse_Vector {
	vector := d.Result.(internedStringVector)
	samples := make([]*querypb.Sample, len(vector))

	for i, s := range vector {
		metricSymbols := make([]uint64, len(s.MetricSymbols))

		for i, s := range s.MetricSymbols {
			metricSymbols[i] = uint64(s)
		}

		samples[i] = &querypb.Sample{
			MetricSymbols: metricSymbols,
			Value:         float64(s.Value),
			Timestamp:     int64(s.Timestamp),
		}
	}

	return &querypb.QueryResponse_Vector{
		Vector: &querypb.VectorData{
			Symbols: d.Symbols,
			Samples: samples,
		},
	}
}

func convertToProtobufScalar(d internedStringsData) (*querypb.QueryResponse_Scalar, error) {
	var originalScalar model.Scalar
	if err := json.Unmarshal(d.Result.(json.RawMessage), &originalScalar); err != nil {
		return nil, fmt.Errorf("could not decode scalar result: %w", err)
	}

	return &querypb.QueryResponse_Scalar{
		Scalar: &querypb.ScalarData{
			Timestamp: int64(originalScalar.Timestamp),
			Value:     float64(originalScalar.Value),
		},
	}, nil
}
