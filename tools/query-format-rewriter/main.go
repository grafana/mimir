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
	"github.com/grafana/mimir/pkg/querier/uninternedquerypb"
)

func main() {
	workingDir := ""
	flag.StringVar(&workingDir, "working-dir", "", "Directory containing files in original JSON query format in a subdirectory named 'original-format'. Rewritten files will written to other subdirectories.")
	flag.Parse()

	if workingDir == "" {
		fmt.Println("Missing required command line flag: working directory")
		os.Exit(1)
	}

	if err := run(workingDir); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run(workingDir string) error {
	originalFormatDir := path.Join(workingDir, "original-format")
	originalFormatFilesInRootDirectory, err := filepath.Glob(path.Join(originalFormatDir, "*.json"))
	if err != nil {
		return fmt.Errorf("listing original format files: %w", err)
	}

	originalFormatFilesInSubdirectories, err := filepath.Glob(path.Join(originalFormatDir, "**", "*.json"))
	if err != nil {
		return fmt.Errorf("listing original format files: %w", err)
	}

	originalFormatFiles := append(originalFormatFilesInRootDirectory, originalFormatFilesInSubdirectories...)
	slices.Sort(originalFormatFiles)

	uninternedProtobufDir := path.Join(workingDir, "uninterned-protobuf")
	internedProtobufDir := path.Join(workingDir, "interned-protobuf")

	for _, d := range []string{uninternedProtobufDir, internedProtobufDir} {
		if err := ensureCreatedAndClean(d); err != nil {
			return fmt.Errorf("creating output directory %v: %w", d, err)
		}
	}

	fmt.Println("Name\tOriginal number of strings\tUnique strings\tOriginal size (bytes)\tUninterned Protobuf format size(bytes)\tInterned Protobuf format size (bytes)")

	for _, originalFormatFile := range originalFormatFiles {
		originalFormat, originalFormatSize, err := parseOriginalFormat(originalFormatFile)

		if err != nil {
			return fmt.Errorf("reading %v: %w", originalFormatFile, err)
		}

		uninternedProtobufBytes, err := convertToUninternedProtobufResponse(originalFormat)

		if err != nil {
			return fmt.Errorf("converting to uninterned Protobuf: %w", err)
		}

		internedProtobufBytes, originalStringCount, uniqueStringCount, err := convertToInternedProtobufResponse(originalFormat)

		if err != nil {
			return fmt.Errorf("converting to interned Protobuf: %w", err)
		}

		relativeName, err := filepath.Rel(originalFormatDir, originalFormatFile)

		if err != nil {
			return fmt.Errorf("determining relative path: %w", err)
		}

		uninternedProtobufFile := path.Join(uninternedProtobufDir, relativeName+".pb")

		if err := createParentAndWriteFile(uninternedProtobufFile, uninternedProtobufBytes); err != nil {
			return fmt.Errorf("writing file %v: %w", uninternedProtobufFile, err)
		}

		internedProtobufFile := path.Join(internedProtobufDir, relativeName+".pb")

		if err := createParentAndWriteFile(internedProtobufFile, internedProtobufBytes); err != nil {
			return fmt.Errorf("writing file %v: %w", internedProtobufFile, err)
		}

		fmt.Printf("%v\t%v\t%v\t%v\t%v\t%v\n", relativeName, originalStringCount, uniqueStringCount, originalFormatSize, len(uninternedProtobufBytes), len(internedProtobufBytes))
	}

	return nil
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

func convertToUninternedProtobufResponse(r originalFormatAPIResponse) (b []byte, err error) {
	resp := uninternedquerypb.QueryResponse{
		Status:    r.Status,
		ErrorType: r.ErrorType,
		Error:     r.Error,
	}

	switch r.Data.Type {
	case model.ValVector:
		data, err := convertToUninternedProtobufVector(r.Data)

		if err != nil {
			return nil, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, err
		}

		return b, nil

	case model.ValScalar:
		data, err := convertToUninternedProtobufScalar(r.Data)

		if err != nil {
			return nil, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, err
		}

		return b, nil

	case model.ValMatrix:
		data, err := convertToUninternedProtobufMatrix(r.Data)

		if err != nil {
			return nil, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, err
		}

		return b, nil

	default:
		panic(fmt.Sprintf("unsupported data type: %v", r.Data.Type))
	}
}

func convertToUninternedProtobufVector(original originalFormatData) (v *uninternedquerypb.QueryResponse_Vector, err error) {
	var originalVector model.Vector
	if err := json.Unmarshal(original.Result, &originalVector); err != nil {
		return nil, fmt.Errorf("could not decode vector result: %w", err)
	}

	samples := make([]*uninternedquerypb.VectorSample, len(originalVector))

	for i, originalSample := range originalVector {
		// This is somewhat convoluted: we do this to ensure we emit the labels in a stable order.
		// (labels.Builder.Labels() sorts the labels before returning the built label set.)
		lb := labels.NewBuilder(labels.EmptyLabels())
		for n, v := range originalSample.Metric {
			lb.Set(string(n), string(v))
		}

		metric := make([]string, 0, len(originalSample.Metric)*2)

		for _, l := range lb.Labels(nil) {
			// FIXME: assign directly to indices of metric rather than using append
			metric = append(metric, l.Name, l.Value)
		}

		samples[i] = &uninternedquerypb.VectorSample{
			Value:     float64(originalSample.Value),
			Timestamp: int64(originalSample.Timestamp),
			Metric:    metric,
		}
	}

	return &uninternedquerypb.QueryResponse_Vector{
		Vector: &uninternedquerypb.VectorData{
			Samples: samples,
		},
	}, nil
}

func convertToUninternedProtobufScalar(d originalFormatData) (*uninternedquerypb.QueryResponse_Scalar, error) {
	var originalScalar model.Scalar
	if err := json.Unmarshal(d.Result, &originalScalar); err != nil {
		return nil, fmt.Errorf("could not decode scalar result: %w", err)
	}

	return &uninternedquerypb.QueryResponse_Scalar{
		Scalar: &uninternedquerypb.ScalarData{
			Timestamp: int64(originalScalar.Timestamp),
			Value:     float64(originalScalar.Value),
		},
	}, nil
}

func convertToUninternedProtobufMatrix(original originalFormatData) (v *uninternedquerypb.QueryResponse_Matrix, err error) {
	var originalMatrix model.Matrix
	if err := json.Unmarshal(original.Result, &originalMatrix); err != nil {
		return nil, fmt.Errorf("could not decode matrix result: %w", err)
	}

	series := make([]*uninternedquerypb.MatrixSeries, 0, len(originalMatrix))

	for _, originalStream := range originalMatrix {
		// This is somewhat convoluted: we do this to ensure we emit the labels in a stable order.
		// (labels.Builder.Labels() sorts the labels before returning the built label set.)
		lb := labels.NewBuilder(labels.EmptyLabels())
		for n, v := range originalStream.Metric {
			lb.Set(string(n), string(v))
		}

		metric := make([]string, 0, len(originalStream.Metric)*2)

		for _, l := range lb.Labels(nil) {
			// FIXME: assign directly to indices of metric rather than using append
			metric = append(metric, l.Name, l.Value)
		}

		samples := make([]*uninternedquerypb.MatrixSample, 0, len(originalStream.Values))

		for _, s := range originalStream.Values {
			// FIXME: assign directly to indices of sample rather than using append
			samples = append(samples, &uninternedquerypb.MatrixSample{
				Value:     float64(s.Value),
				Timestamp: int64(s.Timestamp),
			})
		}

		// FIXME: assign directly to indices of series rather than using append
		series = append(series, &uninternedquerypb.MatrixSeries{
			Metric:  metric,
			Samples: samples,
		})
	}

	return &uninternedquerypb.QueryResponse_Matrix{
		Matrix: &uninternedquerypb.MatrixData{
			Series: series,
		},
	}, nil
}

func convertToInternedProtobufResponse(r originalFormatAPIResponse) (b []byte, originalStringCount int, uniqueStringCount int, err error) {
	resp := internedquerypb.QueryResponse{
		Status:    r.Status,
		ErrorType: r.ErrorType,
		Error:     r.Error,
	}

	switch r.Data.Type {
	case model.ValVector:
		data, originalStringCount, uniqueStringCount, err := convertToInternedProtobufVector(r.Data)

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
		data, err := convertToInternedProtobufScalar(r.Data)

		if err != nil {
			return nil, 0, 0, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, 0, 0, err
		}

		return b, 0, 0, nil

	case model.ValMatrix:
		data, originalStringCount, uniqueStringCount, err := convertToInternedProtobufMatrix(r.Data)

		if err != nil {
			return nil, 0, 0, err
		}

		resp.Data = data
		b, err := resp.Marshal()

		if err != nil {
			return nil, 0, 0, err
		}

		return b, originalStringCount, uniqueStringCount, nil

	default:
		panic(fmt.Sprintf("unsupported data type: %v", r.Data.Type))
	}
}

func convertToInternedProtobufVector(original originalFormatData) (v *internedquerypb.QueryResponse_Vector, originalStringCount int, uniqueStringCount int, err error) {
	var originalVector model.Vector
	if err := json.Unmarshal(original.Result, &originalVector); err != nil {
		return nil, 0, 0, fmt.Errorf("could not decode vector result: %w", err)
	}

	invertedSymbols := map[string]uint64{}
	samples := make([]*internedquerypb.VectorSample, len(originalVector))
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

			// FIXME: assign directly to indices of metricSymbols rather than using append
			metricSymbols = append(metricSymbols, invertedSymbols[l.Name], invertedSymbols[l.Value])
			originalStringCount += 2
		}

		samples[i] = &internedquerypb.VectorSample{
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

func convertToInternedProtobufScalar(d originalFormatData) (*internedquerypb.QueryResponse_Scalar, error) {
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

func convertToInternedProtobufMatrix(original originalFormatData) (v *internedquerypb.QueryResponse_Matrix, originalStringCount int, uniqueStringCount int, err error) {
	var originalMatrix model.Matrix
	if err := json.Unmarshal(original.Result, &originalMatrix); err != nil {
		return nil, 0, 0, fmt.Errorf("could not decode vector result: %w", err)
	}

	invertedSymbols := map[string]uint64{}
	series := make([]*internedquerypb.MatrixSeries, len(originalMatrix))
	originalStringCount = 0

	for i, originalSeries := range originalMatrix {
		// This is somewhat convoluted: we do this to ensure we emit the labels in a stable order.
		// (labels.Builder.Labels() sorts the labels before returning the built label set.)
		lb := labels.NewBuilder(labels.EmptyLabels())
		for n, v := range originalSeries.Metric {
			lb.Set(string(n), string(v))
		}

		metricSymbols := make([]uint64, 0, len(originalSeries.Metric)*2)

		for _, l := range lb.Labels(nil) {
			if _, ok := invertedSymbols[l.Name]; !ok {
				invertedSymbols[l.Name] = uint64(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[l.Value]; !ok {
				invertedSymbols[l.Value] = uint64(len(invertedSymbols))
			}

			// FIXME: assign directly to indices of metricSymbols rather than using append
			metricSymbols = append(metricSymbols, invertedSymbols[l.Name], invertedSymbols[l.Value])
			originalStringCount += 2
		}

		samples := make([]*internedquerypb.MatrixSample, 0, len(originalSeries.Values))

		for _, s := range originalSeries.Values {
			samples = append(samples, &internedquerypb.MatrixSample{
				Value:     float64(s.Value),
				Timestamp: int64(s.Timestamp),
			})
		}

		series[i] = &internedquerypb.MatrixSeries{
			MetricSymbols: metricSymbols,
			Samples:       samples,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	uniqueStringCount = len(symbols)

	return &internedquerypb.QueryResponse_Matrix{
		Matrix: &internedquerypb.MatrixData{
			Symbols: symbols,
			Series:  series,
		},
	}, originalStringCount, uniqueStringCount, nil
}
