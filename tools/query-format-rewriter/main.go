// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding"
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

	fmt.Println("Name\tOriginal size (bytes)\tUninterned Protobuf format size(bytes)\tInterned Protobuf format size (bytes)")

	for _, originalFormatFile := range originalFormatFiles {
		originalFormat, originalFormatSize, err := parseOriginalFormat(originalFormatFile)

		if err != nil {
			return fmt.Errorf("reading %v: %w", originalFormatFile, err)
		}

		uninternedProtobufBytes, err := convertToUninternedProtobufResponse(originalFormat)

		if err != nil {
			return fmt.Errorf("converting to uninterned Protobuf: %w", err)
		}

		internedProtobufBytes, err := convertToInternedProtobufResponse(originalFormat)

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

		fmt.Printf("%v\t%v\t%v\t%v\n", relativeName, originalFormatSize, len(uninternedProtobufBytes), len(internedProtobufBytes))
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

func parseOriginalFormat(f string) (querymiddleware.PrometheusResponse, int, error) {
	b, err := os.ReadFile(f)

	if err != nil {
		return querymiddleware.PrometheusResponse{}, 0, err
	}

	codec := encoding.OriginalJsonCodec{}
	resp, err := codec.Decode(b)

	if err != nil {
		return querymiddleware.PrometheusResponse{}, 0, err
	}

	return resp, len(b), nil
}
func convertToUninternedProtobufResponse(r querymiddleware.PrometheusResponse) ([]byte, error) {
	codec := encoding.UninternedProtobufCodec{}
	b, err := codec.Encode(r)

	if err != nil {
		return nil, err
	}

	return b, nil
}

func convertToInternedProtobufResponse(r querymiddleware.PrometheusResponse) ([]byte, error) {
	codec := encoding.InternedProtobufCodec{}
	b, err := codec.Encode(r)

	if err != nil {
		return nil, err
	}

	return b, nil
}
