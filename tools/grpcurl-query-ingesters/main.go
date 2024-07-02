// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	startT := int64(0)
	endT := int64(0)

	flag.Int64Var(&startT, "start", 0, "Start time for analysis (inclusive, milliseconds since epoch)")
	flag.Int64Var(&endT, "end", 0, "End time for analysis (inclusive, milliseconds since epoch)")
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Println("Failed to parse CLI arguments:", err.Error())
		os.Exit(1)
	}

	if startT == 0 || endT == 0 {
		fmt.Println("Missing start or end time")
		os.Exit(1)
	}

	if startT >= endT {
		fmt.Println("Invalid time range")
		os.Exit(1)
	}

	start := time.UnixMilli(startT).UTC()
	end := time.UnixMilli(endT).UTC()

	for _, arg := range args {
		res, err := parseFile(arg)
		if err != nil {
			fmt.Println("Failed to parse file:", err.Error())
			os.Exit(1)
		}

		dumpResponse(res, start, end)
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

const writeInterval = 20 * time.Second

func dumpResponse(res QueryStreamResponse, startT, endT time.Time) {
	for _, series := range res.Chunkseries {
		expectedPointCount := (endT.Sub(startT).Milliseconds() / writeInterval.Milliseconds()) + 1

		fmt.Println(series.LabelSet().String())
		presentPointCount := int64(0)

		for _, chunk := range series.Chunks {
			if chunk.EndTime().Before(startT) {
				continue
			}

			if chunk.StartTime().After(endT) {
				continue
			}

			chunkPoints := 0

			fmt.Printf(
				"- Chunk: %s (%v) - %s (%v)\n",
				chunk.StartTime().Format(time.TimeOnly),
				chunk.StartTime().UnixMilli(),
				chunk.EndTime().Format(time.TimeOnly),
				chunk.EndTime().UnixMilli(),
			)

			chunkIterator := chunk.EncodedChunk().NewIterator(nil)
			for {
				sampleType := chunkIterator.Scan()
				if sampleType == chunkenc.ValNone {
					break
				}

				if sampleType != chunkenc.ValFloat {
					panic(fmt.Errorf("unknown sample type %s", sampleType.String()))
				}

				chunkPoints++

				ts := time.UnixMilli(chunkIterator.Timestamp()).UTC()
				f := float64(chunkIterator.Value().Value)

				if !ts.Equal(chunkIterator.Value().Timestamp.Time()) {
					fmt.Printf("  - Iterator timestamp (%v) != value timestamp (%v)!\n", ts, chunkIterator.Value().Timestamp)
				}

				if ts.Equal(startT) || ts.Equal(endT) || (ts.After(startT) && ts.Before(endT)) {
					presentPointCount++
					expectedValue := generateSineWaveValue(ts)

					if math.Abs(f-expectedValue) > 0.0001 {
						fmt.Printf("  - at t=%v, expected value %v, but chunk has %v!\n", ts.Format(time.RFC3339), expectedValue, f)
					}

					//fmt.Printf("  - t=%v, value is %v\n", ts.Format(time.RFC3339), f)
				}
			}

			if chunkIterator.Err() != nil {
				panic(chunkIterator.Err())
			}

			fmt.Printf("  - Total points: %v\n", chunkPoints)
		}

		missingPointCount := expectedPointCount - presentPointCount

		if missingPointCount != 0 {
			fmt.Printf("- Expected to see %v point(s) between %v and %v, but chunks only have %v point(s)\n", expectedPointCount, startT.Format(time.RFC3339), endT.Format(time.RFC3339), presentPointCount)
		}
	}
}
func generateSineWaveValue(t time.Time) float64 {
	period := 10 * time.Minute
	radians := 2 * math.Pi * float64(t.UnixNano()) / float64(period.Nanoseconds())
	return math.Sin(radians)
}
