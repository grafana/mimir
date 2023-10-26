package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"

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

	for _, file := range args {
		readSamplingFile(file)
	}
}

func readSamplingFile(file string) {
	fd, err := os.Open(file)
	if err != nil {
		fmt.Println("Failed to open file", file)
		return
	}

	// Ensure the file will get closed once done.
	defer fd.Close()

	for {
		err := readNextRequestFromFile(fd)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Failed to read request from file:", err.Error())
			break
		}
	}
}

func readNextRequestFromFile(fd *os.File) error {
	var size uint64

	// Read the request size.
	if err := binary.Read(fd, binary.BigEndian, &size); err != nil {
		return errors.Wrap(err, "read request size")
	}

	// Read the request.
	buffer := make([]byte, size)
	if _, err := io.ReadFull(fd, buffer); err != nil {
		return errors.Wrap(err, "read request data")
	}

	// Decode the request.
	req := mimirpb.WriteRequest{}
	if err := req.Unmarshal(buffer); err != nil {
		return errors.Wrap(err, "unmarshal request data")
	}

	fmt.Println("Request with", len(req.Timeseries), "timeseries,", len(req.Metadata), "metadata")
	return nil
}
