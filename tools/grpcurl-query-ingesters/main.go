// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
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
		res, err := parseFile(arg)
		if err != nil {
			fmt.Println("Failed to parse file:", err.Error())
			os.Exit(1)
		}

		dumpResponse(res)
	}
}

func parseFile(file string) (QueryStreamResponse, error) {
	res := QueryStreamResponse{}

	fileData, err := os.ReadFile(file)
	if err != nil {
		return res, err
	}

	// Decode file.
	decoder := json.NewDecoder(bytes.NewReader(fileData))
	if err := decoder.Decode(&res); err != nil {
		return res, err
	}

	return res, nil
}

func dumpResponse(res QueryStreamResponse) {
	for _, series := range res.Chunkseries {
		fmt.Println(series.LabelSet().String())
		var (
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ts int64
		)

		for _, chunk := range series.Chunks {
			fmt.Printf(
				"- Chunk: %s - %s\n",
				chunk.StartTime().Format(time.TimeOnly),
				chunk.EndTime().Format(time.TimeOnly))

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
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", h)
				case chunkenc.ValFloatHistogram:
					ts, fh := chunkIterator.AtFloatHistogram(fh)
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", fh)
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
