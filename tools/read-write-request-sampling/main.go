package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Println("Failed to parse CLI flags:", err.Error())
		os.Exit(1)
	}
	if len(args) == 0 {
		fmt.Println("No file specified in the command line")
		os.Exit(1)
	}

	// Read all requests from all files.
	var reqs []*mimirpb.WriteRequest
	for _, file := range args {
		got, err := readRequestsFromFile(file)

		// An error may occur after we've read something, so better to not terminate
		// the process but analyse what we've got.
		if err != nil {
			fmt.Println("Failed to read requests from file", file, err.Error())
		}

		reqs = append(reqs, got...)
	}

	// Analysis.
	analyzeLabelsRepetitionFromRequests(reqs)
	analyzeLabelsSizeFromRequests(reqs)
	analyzeCompression(reqs)
}

func readRequestsFromFile(file string) ([]*mimirpb.WriteRequest, error) {
	var reqs []*mimirpb.WriteRequest

	fd, err := os.Open(file)
	if err != nil {
		return reqs, errors.Wrap(err, "open file")
	}

	// Ensure the file will get closed once done.
	defer fd.Close()

	for {
		req, err := readNextRequestFromFile(fd)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return reqs, errors.Wrap(err, "read request from file")
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func readNextRequestFromFile(fd *os.File) (*mimirpb.WriteRequest, error) {
	var size uint64

	// Read the request size.
	if err := binary.Read(fd, binary.BigEndian, &size); err != nil {
		return nil, errors.Wrap(err, "read request size")
	}

	// Read the request.
	buffer := make([]byte, size)
	if _, err := io.ReadFull(fd, buffer); err != nil {
		return nil, errors.Wrap(err, "read request data")
	}

	// Decode the request.
	req := &mimirpb.WriteRequest{}
	if err := req.Unmarshal(buffer); err != nil {
		return nil, errors.Wrap(err, "unmarshal request data")
	}

	return req, nil
}

func analyzeLabelsRepetitionFromRequests(reqs []*mimirpb.WriteRequest) {
	for _, req := range reqs {
		analyzeLabelsRepetitionFromRequest(req)
	}
}

func analyzeLabelsRepetitionFromRequest(req *mimirpb.WriteRequest) {
	var (
		names       = map[string]int{}
		values      = map[string]int{}
		labelsCount int
	)

	// Count label name occurrences.
	for _, ts := range req.Timeseries {
		for _, label := range ts.Labels {
			names[label.Name]++
			values[label.Value]++
			labelsCount++
		}
	}

	// Compute the average number of repetitions.
	namesRepetition := 1 - (float64(len(names)) / float64(labelsCount))
	valuesRepetition := 1 - (float64(len(values)) / float64(labelsCount))

	fmt.Println(fmt.Sprintf("Names repetition: %.2f values repetition: %.2f", namesRepetition, valuesRepetition))
}

func analyzeLabelsSizeFromRequests(reqs []*mimirpb.WriteRequest) {
	var totalOrigSize, totalOrigWithoutLabelsSize int

	for _, req := range reqs {
		origSize, origWithoutLabelsSize := analyzeLabelsSizeFromRequest(req)

		totalOrigSize += origSize
		totalOrigWithoutLabelsSize += origWithoutLabelsSize
	}

	labelsImpactPerc := 1 - (float64(totalOrigWithoutLabelsSize) / float64(totalOrigSize))
	fmt.Println(fmt.Sprintf("Labels are %.2f%% of protobuf encoded request", labelsImpactPerc*100))
}

func analyzeLabelsSizeFromRequest(req *mimirpb.WriteRequest) (origSize, origWithoutLabelsSize int) {
	// Encode the request as is.
	origEncoded, _ := req.Marshal()

	// Decode the request to get a clone.
	clone := &mimirpb.WriteRequest{}
	panicOnError(clone.Unmarshal(origEncoded))

	// Remove all labels.
	for i := 0; i < len(clone.Timeseries); i++ {
		clone.Timeseries[i].SetLabels(nil)
	}

	// Re-encode the version without labels.
	cloneEncoded, _ := clone.Marshal() // TODO check error

	origSize = len(origEncoded)
	origWithoutLabelsSize = len(cloneEncoded)
	return
}

func analyzeCompression(reqs []*mimirpb.WriteRequest) {
	var (
		totalOrigSize   int
		totalGzipSize   int
		totalSnappySize int
		totalZlibSize   int
		totalFlateSize  int
		buffer          = bytes.NewBuffer(nil)
	)

	for _, req := range reqs {
		// Encode the request as is.
		origEncoded, _ := req.Marshal() // TODO error
		totalOrigSize += len(origEncoded)

		// Gzip.
		totalGzipSize += compressAndReturnSize(origEncoded, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
			return gzip.NewWriter(buffer)
		})

		// Snappy.
		totalSnappySize += compressAndReturnSize(origEncoded, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
			return snappy.NewBufferedWriter(buffer)
		})

		// Zlib.
		totalZlibSize += compressAndReturnSize(origEncoded, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
			return zlib.NewWriter(buffer)
		})

		// Flate.
		totalFlateSize += compressAndReturnSize(origEncoded, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
			writer, err := flate.NewWriter(buffer, flate.DefaultCompression)
			panicOnError(err)
			return writer
		})
	}

	gzipRatio := float64(totalOrigSize) / float64(totalGzipSize)
	snappyRatio := float64(totalOrigSize) / float64(totalSnappySize)
	zlibRatio := float64(totalOrigSize) / float64(totalZlibSize)
	flateRatio := float64(totalOrigSize) / float64(totalFlateSize)

	fmt.Println(fmt.Sprintf("Per-request compression ratios: gzip = %.2f snappy = %.2f zlib = %.2f flate = %.2f", gzipRatio, snappyRatio, zlibRatio, flateRatio))
}

func compressAndReturnSize(data []byte, tmpBuffer *bytes.Buffer, getCompressor func(buffer *bytes.Buffer) io.WriteCloser) int {
	tmpBuffer.Reset()
	writer := getCompressor(tmpBuffer)
	panicOnError(writer.Write(data))
	panicOnError(writer.Close())

	return tmpBuffer.Len()
}

func panicOnError(args ...any) {
	for _, arg := range args {
		if err, ok := arg.(error); ok && err != nil {
			panic(err)
		}
	}
}
