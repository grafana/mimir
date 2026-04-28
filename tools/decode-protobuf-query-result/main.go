// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var logger = log.NewLogfmtLogger(os.Stdout)

func main() {
	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	for _, f := range args {
		err := decode(f)
		if err != nil {
			logger.Log("filename", f, "err", err)
			os.Exit(1)
		}
	}
}

func decode(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	formatter := querymiddleware.ProtobufFormatter{}
	resp, err := formatter.DecodeQueryResponse(data)
	if err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}

	switch resp.Data.ResultType {
	case "matrix", "vector":
	default:
		return fmt.Errorf("unsupported result type %v", resp.Data.ResultType)
	}
	fmt.Printf("Decoding %s as %s\n", filename, resp.Data.ResultType)

	for i, stream := range resp.Data.Result {
		identifier := mimirpb.FromLabelAdaptersToString(stream.Labels)
		fmt.Printf("Stream %d, labels %s:\n", i, identifier)
		for _, sample := range stream.Samples {
			fmt.Printf("  Timestamp: %d, Float: %v\n", sample.TimestampMs, sample.Value)
		}
		for _, sample := range stream.Histograms {
			fmt.Printf("  Timestamp: %d, Histogram: %v\n", sample.TimestampMs, sample.Histogram)
		}
	}
	return nil
}
