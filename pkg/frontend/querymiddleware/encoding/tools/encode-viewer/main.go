// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding"
)

func main() {
	sourceFile := ""
	codecName := ""
	flag.StringVar(&sourceFile, "source-file", "", "Path to file containing JSON-encoded query result")
	flag.StringVar(&codecName, "codec", "", "Codec to use")
	flag.Parse()

	if err := run(sourceFile, codecName); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Error: %v\n", err))
		os.Exit(1)
	}
}

func run(sourceFile string, codecName string) error {
	if sourceFile == "" {
		return errors.New("source-file flag is required")
	}

	if codecName == "" {
		return errors.New("codec flag is required")
	}

	sourceBytes, err := os.ReadFile(sourceFile)
	if err != nil {
		return fmt.Errorf("could not read %v: %w", sourceFile, err)
	}

	payload, err := encoding.OriginalJsonCodec{}.Decode(sourceBytes)
	if err != nil {
		return fmt.Errorf("could not decode JSON-encoded query result: %w", err)
	}

	codec, ok := encoding.KnownCodecs[codecName]
	if !ok {
		return fmt.Errorf("unknown codec '%v'", codecName)
	}

	encodedBytes, err := codec.Encode(payload)
	if err != nil {
		return fmt.Errorf("could not encode query result with selected codec: %w", err)
	}

	if _, err := os.Stdout.Write(encodedBytes); err != nil {
		return fmt.Errorf("could not write result to stdout: %w", err)
	}

	return nil
}
