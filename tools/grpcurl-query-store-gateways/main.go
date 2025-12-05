// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Println("Failed to parse CLI arguments:", err.Error())
		os.Exit(1)
	}

	for _, arg := range args {
		response, err := parseFile(arg)
		if err != nil {
			fmt.Println("Failed to parse file:", err.Error())
			os.Exit(1)
		}

		dumpResponse(response, arg)
	}
}

func parseFile(file string) (*SeriesResponse, error) {
	fileData, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	consolidated := SeriesResponse{
		StreamingSeries: &StreamingSeriesResponse{Series: []*Series{}},
		StreamingChunks: &StreamingChunksResponse{Series: []*Series{}},
	}

	// Decode file.
	decoder := json.NewDecoder(bytes.NewReader(fileData))
	for {
		res := SeriesResponse{}
		if err := decoder.Decode(&res); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// merge all the elements back into the single object
		// the raw file has unique blocks for each element
		// { ... }
		// { ... }
		// this is deserialized into a collection of SeriesResponse
		// and each element has one section set.
		if len(res.Hints) > 0 {
			consolidated.Hints = res.Hints
		}

		if len(res.Warning) > 0 {
			consolidated.Warning = res.Warning
		}

		if res.StreamingSeries != nil && len(res.StreamingSeries.Series) > 0 {
			consolidated.StreamingSeries.Series = append(consolidated.StreamingSeries.Series, res.StreamingSeries.Series...)
		}

		if res.StreamingChunks != nil && len(res.StreamingChunks.Series) > 0 {
			consolidated.StreamingChunks.Series = append(consolidated.StreamingChunks.Series, res.StreamingChunks.Series...)
		}
	}

	return &consolidated, nil
}

func dumpResponse(res *SeriesResponse, file string) {
	if res.Warning != "" {
		fmt.Printf("Warning: %s\n", res.Warning)
		return
	}

	if res.StreamingSeries == nil || res.StreamingChunks == nil || len(res.StreamingChunks.Series) == 0 || len(res.StreamingSeries.Series) != len(res.StreamingChunks.Series) {
		return
	}

	fmt.Printf("---- %s ----\n", file)

	for i, series := range res.StreamingSeries.Series {

		if series == nil || res.StreamingChunks.Series[i] == nil {
			continue
		}

		chunks := res.StreamingChunks.Series[i].Chunks
		fmt.Println(series.LabelSet().String())
		var (
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ts int64
		)

		slices.SortFunc(chunks, func(a, b AggrChunk) int {
			return int(a.StartTimestamp() - b.StartTimestamp())
		})

		for _, chunk := range chunks {
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
					ts, fh = chunkIterator.AtFloatHistogram(fh)
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
