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
	timeseriesReqs := filterTimeseriesRequests(reqs)
	analyzeDataset(timeseriesReqs)
	analyzeLabelsRepetitionFromRequests(timeseriesReqs)
	analyzeLabelsSizeFromRequests(timeseriesReqs)
	analyzeMinimizedWriteRequests(timeseriesReqs)
	analyzeHyperMinimizedWriteRequests(timeseriesReqs)
	analyzeOriginalWriteRequestsCompression(timeseriesReqs)
	analyzeMinimizedWriteRequestsCompression(timeseriesReqs)
	analyzeHyperMinimizedWriteRequestsCompression(timeseriesReqs)
}

// filterTimeseriesRequests returns only requests containing timeseries (some requests contain only metadata).
func filterTimeseriesRequests(reqs []*mimirpb.WriteRequest) []*mimirpb.WriteRequest {
	filtered := make([]*mimirpb.WriteRequest, 0, len(reqs))

	for _, req := range reqs {
		if len(req.Timeseries) == 0 {
			continue
		}

		filtered = append(filtered, req)
	}

	return filtered
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

func analyzeDataset(reqs []*mimirpb.WriteRequest) {
	var (
		seriesCount  float64
		samplesCount float64
	)

	for _, req := range reqs {
		for _, ts := range req.Timeseries {
			seriesCount++
			samplesCount += float64(len(ts.Samples) + len(ts.Histograms))
		}
	}

	fmt.Println("Dataset:")
	fmt.Println("- Number of requests:", len(reqs))
	fmt.Println(fmt.Sprintf("- Average number of series per request: %.2f", seriesCount/float64(len(reqs))))
	fmt.Println(fmt.Sprintf("- Average number of samples per series: %.2f", samplesCount/seriesCount))
	fmt.Println("")
}

func analyzeLabelsRepetitionFromRequests(reqs []*mimirpb.WriteRequest) {
	var (
		namesRepetitionSum  float64
		valuesRepetitionSum float64
	)

	for _, req := range reqs {
		namesRepetition, valuesRepetition := analyzeLabelsRepetitionFromRequest(req)

		namesRepetitionSum += namesRepetition
		valuesRepetitionSum += valuesRepetition
	}

	fmt.Println("Label symbols repetitions:")
	fmt.Println(fmt.Sprintf("- Names:  %.2f", namesRepetitionSum/float64(len(reqs))))
	fmt.Println(fmt.Sprintf("- Values: %.2f", valuesRepetitionSum/float64(len(reqs))))
	fmt.Println("")
}

func analyzeLabelsRepetitionFromRequest(req *mimirpb.WriteRequest) (namesRepetition, valuesRepetition float64) {
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
	namesRepetition = 1 - (float64(len(names)) / float64(labelsCount))
	valuesRepetition = 1 - (float64(len(values)) / float64(labelsCount))
	return
}

func analyzeLabelsSizeFromRequests(reqs []*mimirpb.WriteRequest) {
	var totalOrigSize, totalOrigWithoutLabelsSize int

	for _, req := range reqs {
		origSize, origWithoutLabelsSize := analyzeLabelsSizeFromRequest(req)

		totalOrigSize += origSize
		totalOrigWithoutLabelsSize += origWithoutLabelsSize
	}

	labelsImpactPerc := 1 - (float64(totalOrigWithoutLabelsSize) / float64(totalOrigSize))

	fmt.Println("Labels sized compared to the whole (protobuf encoded) request:")
	fmt.Println(fmt.Sprintf("- Labels are %.2f%% of protobuf encoded request", labelsImpactPerc*100))
	fmt.Println("")
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
	cloneEncoded, err := clone.Marshal()
	panicOnError(err)

	origSize = len(origEncoded)
	origWithoutLabelsSize = len(cloneEncoded)
	return
}

func analyzeOriginalWriteRequestsCompression(reqs []*mimirpb.WriteRequest) {
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
		encoded, err := req.Marshal()
		panicOnError(err)
		totalOrigSize += len(encoded)

		gzipSize, snappySize, zlibSize, flateSize := compressDataWithMultipleAlgorithms(encoded, buffer)

		totalGzipSize += gzipSize
		totalSnappySize += snappySize
		totalZlibSize += zlibSize
		totalFlateSize += flateSize
	}

	fmt.Println("Original write request (protobuf encoded) compression ratios:")
	fmt.Println(fmt.Sprintf("- snappy = %.2f", float64(totalOrigSize)/float64(totalSnappySize)))
	fmt.Println(fmt.Sprintf("- gzip =   %.2f", float64(totalOrigSize)/float64(totalGzipSize)))
	fmt.Println(fmt.Sprintf("- zlib =   %.2f", float64(totalOrigSize)/float64(totalZlibSize)))
	fmt.Println(fmt.Sprintf("- flate =  %.2f", float64(totalOrigSize)/float64(totalFlateSize)))
	fmt.Println("")
}

func analyzeMinimizedWriteRequestsCompression(reqs []*mimirpb.WriteRequest) {
	var (
		totalOrigSize            int
		totalMinimizedSize       int
		totalMinimizedGzipSize   int
		totalMinimizedSnappySize int
		totalMinimizedZlibSize   int
		totalMinimizedFlateSize  int
		buffer                   = bytes.NewBuffer(nil)
	)

	for _, req := range reqs {
		// Encode the original request just to get its size.
		// NOTE: restrict scope to ensure we don't do any mistake and compress the original one.
		{
			origEncoded, err := req.Marshal()
			panicOnError(err)
			totalOrigSize += len(origEncoded)
		}

		// Convert to a minimized request.
		minReq := minimizeWriteRequest(req)

		// Encode the request as is.
		minEncoded, err := minReq.Marshal()
		panicOnError(err)
		totalMinimizedSize += len(minEncoded)

		gzipSize, snappySize, zlibSize, flateSize := compressDataWithMultipleAlgorithms(minEncoded, buffer)

		totalMinimizedGzipSize += gzipSize
		totalMinimizedSnappySize += snappySize
		totalMinimizedZlibSize += zlibSize
		totalMinimizedFlateSize += flateSize
	}

	fmt.Println("Minimized write request (protobuf encoded) compression ratios:")
	fmt.Println("- Compared to original (uncompressed) write request:")
	fmt.Println(fmt.Sprintf("  - snappy = %.2f", float64(totalOrigSize)/float64(totalMinimizedSnappySize)))
	fmt.Println(fmt.Sprintf("  - gzip =   %.2f", float64(totalOrigSize)/float64(totalMinimizedGzipSize)))
	fmt.Println(fmt.Sprintf("  - zlib =   %.2f", float64(totalOrigSize)/float64(totalMinimizedZlibSize)))
	fmt.Println(fmt.Sprintf("  - flate =  %.2f", float64(totalOrigSize)/float64(totalMinimizedFlateSize)))
	fmt.Println("- Compared to minimized (uncompressed) write request:")
	fmt.Println(fmt.Sprintf("  - snappy = %.2f", float64(totalMinimizedSize)/float64(totalMinimizedSnappySize)))
	fmt.Println(fmt.Sprintf("  - gzip =   %.2f", float64(totalMinimizedSize)/float64(totalMinimizedGzipSize)))
	fmt.Println(fmt.Sprintf("  - zlib =   %.2f", float64(totalMinimizedSize)/float64(totalMinimizedZlibSize)))
	fmt.Println(fmt.Sprintf("  - flate =  %.2f", float64(totalMinimizedSize)/float64(totalMinimizedFlateSize)))
	fmt.Println("")
}

func analyzeHyperMinimizedWriteRequestsCompression(reqs []*mimirpb.WriteRequest) {
	var (
		totalOrigSize                 int
		totalHyperMinimizedSize       int
		totalHyperMinimizedGzipSize   int
		totalHyperMinimizedSnappySize int
		totalHyperMinimizedZlibSize   int
		totalHyperMinimizedFlateSize  int
		buffer                        = bytes.NewBuffer(nil)
	)

	for _, req := range reqs {
		// Encode the original request just to get its size.
		// NOTE: restrict scope to ensure we don't do any mistake and compress the original one.
		{
			origEncoded, err := req.Marshal()
			panicOnError(err)
			totalOrigSize += len(origEncoded)
		}

		// Convert to a hyper minimized request.
		minReq := hyperMinimizeWriteRequest(req)

		// Encode the request as is.
		minEncoded, err := minReq.Marshal()
		panicOnError(err)
		totalHyperMinimizedSize += len(minEncoded)

		gzipSize, snappySize, zlibSize, flateSize := compressDataWithMultipleAlgorithms(minEncoded, buffer)

		totalHyperMinimizedGzipSize += gzipSize
		totalHyperMinimizedSnappySize += snappySize
		totalHyperMinimizedZlibSize += zlibSize
		totalHyperMinimizedFlateSize += flateSize
	}

	fmt.Println("Hyper minimized write request (protobuf encoded) compression ratios:")
	fmt.Println("- Compared to original (uncompressed) write request:")
	fmt.Println(fmt.Sprintf("  - snappy = %.2f", float64(totalOrigSize)/float64(totalHyperMinimizedSnappySize)))
	fmt.Println(fmt.Sprintf("  - gzip =   %.2f", float64(totalOrigSize)/float64(totalHyperMinimizedGzipSize)))
	fmt.Println(fmt.Sprintf("  - zlib =   %.2f", float64(totalOrigSize)/float64(totalHyperMinimizedZlibSize)))
	fmt.Println(fmt.Sprintf("  - flate =  %.2f", float64(totalOrigSize)/float64(totalHyperMinimizedFlateSize)))
	fmt.Println("- Compared to minimized (uncompressed) write request:")
	fmt.Println(fmt.Sprintf("  - snappy = %.2f", float64(totalHyperMinimizedSize)/float64(totalHyperMinimizedSnappySize)))
	fmt.Println(fmt.Sprintf("  - gzip =   %.2f", float64(totalHyperMinimizedSize)/float64(totalHyperMinimizedGzipSize)))
	fmt.Println(fmt.Sprintf("  - zlib =   %.2f", float64(totalHyperMinimizedSize)/float64(totalHyperMinimizedZlibSize)))
	fmt.Println(fmt.Sprintf("  - flate =  %.2f", float64(totalHyperMinimizedSize)/float64(totalHyperMinimizedFlateSize)))
	fmt.Println("")
}

func compressDataWithMultipleAlgorithms(data []byte, buffer *bytes.Buffer) (gzipSize, snappySize, zlibSize, flateSize int) {
	// Gzip.
	gzipSize = compressAndReturnSize(data, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
		return gzip.NewWriter(buffer)
	})

	// Snappy.
	snappySize = compressAndReturnSize(data, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
		return snappy.NewBufferedWriter(buffer)
	})

	// Zlib.
	zlibSize = compressAndReturnSize(data, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
		return zlib.NewWriter(buffer)
	})

	// Flate.
	flateSize = compressAndReturnSize(data, buffer, func(buffer *bytes.Buffer) io.WriteCloser {
		writer, err := flate.NewWriter(buffer, flate.DefaultCompression)
		panicOnError(err)
		return writer
	})

	return
}

func compressAndReturnSize(data []byte, tmpBuffer *bytes.Buffer, getCompressor func(buffer *bytes.Buffer) io.WriteCloser) int {
	tmpBuffer.Reset()
	writer := getCompressor(tmpBuffer)
	panicOnError(writer.Write(data))
	panicOnError(writer.Close())

	return tmpBuffer.Len()
}

func analyzeMinimizedWriteRequests(reqs []*mimirpb.WriteRequest) {
	var (
		totalOrigSize      int
		totalMinimizedSize int
	)

	for _, req := range reqs {
		minReq := minimizeWriteRequest(req)

		// Encode the request as is.
		origEncoded, err := req.Marshal()
		panicOnError(err)
		totalOrigSize += len(origEncoded)

		// Encode the minimized request.
		minEncoded, err := minReq.Marshal()
		panicOnError(err)
		totalMinimizedSize += len(minEncoded)
	}

	fmt.Println("Minimized request (per-request symbols table):")
	fmt.Println(fmt.Sprintf("- Total original size:  %d", totalOrigSize))
	fmt.Println(fmt.Sprintf("- Total minimized size: %d", totalMinimizedSize))
	fmt.Println(fmt.Sprintf("- Reduction ratio:      %.2f", float64(totalOrigSize)/float64(totalMinimizedSize)))
	fmt.Println("")
}

func analyzeHyperMinimizedWriteRequests(reqs []*mimirpb.WriteRequest) {
	var (
		totalOrigSize           int
		totalHyperMinimizedSize int
	)

	for _, req := range reqs {
		minReq := hyperMinimizeWriteRequest(req)

		// Encode the request as is.
		origEncoded, err := req.Marshal()
		panicOnError(err)
		totalOrigSize += len(origEncoded)

		// Encode the hyper minimized request.
		minEncoded, err := minReq.Marshal()
		panicOnError(err)
		totalHyperMinimizedSize += len(minEncoded)
	}

	fmt.Println("Hyper minimized request (per-request symbols table):")
	fmt.Println(fmt.Sprintf("- Total original size:        %d", totalOrigSize))
	fmt.Println(fmt.Sprintf("- Total hyper minimized size: %d", totalHyperMinimizedSize))
	fmt.Println(fmt.Sprintf("- Reduction ratio:            %.2f", float64(totalOrigSize)/float64(totalHyperMinimizedSize)))
	fmt.Println("")
}

func panicOnError(args ...any) {
	for _, arg := range args {
		if err, ok := arg.(error); ok && err != nil {
			panic(err)
		}
	}
}
