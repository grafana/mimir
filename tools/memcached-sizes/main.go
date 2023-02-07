// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// This script computes the size that a specific percentile of entries in a Memcached
// server fit within. For example, it could determine that 99.9% of entries in a server
// are 512 bytes or fewer.
//
// It operates on the output of the `stats  sizes` command from a Memcached server, read
// from standard input. In order to get this information the following things are required:
//
// * Run Memcached with the `--extended=track_sizes` flag
// * Port forward to a Memcached instance: `kubectl port-forward --namespace=example memcached-0 11211:11211`
// * Grab the stats: `echo 'stats sizes' | nc -N localhost 11211 > sizes.txt`
// * Run this script: `go run tools/memcached-sizes/main.go --percentile=0.999 < sizes.txt`

const defaultPercentile = 0.99

type config struct {
	percentile float64
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&c.percentile, "percentile", defaultPercentile, "Percentile of entries to compute the size for (0 to 1.0).")
}

func (c *config) validate() error {
	if c.percentile > 1.0 || c.percentile < 0.0 {
		return fmt.Errorf("invalid percentile (must be from 0.0 to 1.0): %f", c.percentile)
	}

	return nil
}

type stat struct {
	size  uint64
	count uint64
}

func main() {
	logger := log.WithPrefix(log.NewLogfmtLogger(os.Stderr), "time", log.DefaultTimestampUTC)

	cfg := config{}
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	if err := cfg.validate(); err != nil {
		level.Error(logger).Log("msg", "invalid configuration", "err", err)
		os.Exit(1)
	}

	stats, total, err := readLines(os.Stdin)
	if err != nil {
		level.Error(logger).Log("msg", "unable to parse size stats from stdin", "err", err)
		os.Exit(1)
	}

	percentileCount := uint64(float64(total) * cfg.percentile)
	var percentileSize uint64
	var lookedAt uint64

	for _, s := range stats {
		lookedAt += s.count
		if lookedAt >= percentileCount {
			percentileSize = s.size
			break
		}
	}

	fmt.Printf("Item size for p %f (%d of %d samples) is %d\n", cfg.percentile, percentileCount, total, percentileSize)

}

func readLines(r io.Reader) ([]stat, uint64, error) {
	var stats []stat
	var total uint64
	buf := bufio.NewReader(r)

	for {
		bytes, _, err := buf.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, 0, err
		}

		line := string(bytes)
		if line == "END" {
			break
		}

		s, err := parseStat(line)
		if err != nil {
			return nil, 0, err
		}

		stats = append(stats, s)
		total += s.count
	}

	return stats, total, nil
}

func parseStat(line string) (stat, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return stat{}, fmt.Errorf("unexpected number of parts from line %s", line)
	}

	size, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return stat{}, fmt.Errorf("unable to parse size in stat line %s: %w", line, err)
	}

	count, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return stat{}, fmt.Errorf("unable to parse count in stat line %s: %w", line, err)
	}

	return stat{
		size:  size,
		count: count,
	}, nil
}
