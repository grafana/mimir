// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var logger = log.NewLogfmtLogger(os.Stdout)

func main() {
	samples := flag.Bool("samples", false, "Print samples in chunks")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	for _, f := range args {
		err := printChunksFile(f, *samples)
		if err != nil {
			logger.Log("filename", f, "err", err)
		}
	}
}

func printChunksFile(filename string, printSamples bool) error {
	fmt.Println(filename)

	file, err := os.Open(filename)  //#nosec G109 -- this is intentionally taking operator input, not an injection.

	if err != nil {
		return err
	}
	defer file.Close()

	header := make([]byte, 8)
	n, err := io.ReadFull(file, header)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	if n != 8 {
		return fmt.Errorf("failed to read header: %d bytes read only", n)
	}

	if binary.BigEndian.Uint32(header) != chunks.MagicChunks {
		return fmt.Errorf("file doesn't start with magic prefix")
	}
	if header[4] != 0x01 {
		return fmt.Errorf("invalid version: 0x%02x", header[4])
	}

	if !bytes.Equal(header[5:], []byte{0, 0, 0}) {
		return fmt.Errorf("invalid padding")
	}

	cix := 0
	var (
		h  *histogram.Histogram      // reused in iteration as we just dump the value and move on
		fh *histogram.FloatHistogram // reused in iteration as we just dump the value and move on
		ts int64                     // we declare ts here to prevent shadowing of h and fh within the loop
	)
	for c, err := nextChunk(cix, file); err == nil; c, err = nextChunk(cix, file) {
		if printSamples {
			minTS := int64(math.MaxInt64)
			maxTS := int64(0)

			it := c.Iterator(nil)
			six := 0
			for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
				switch valType {
				case chunkenc.ValFloat:
					ts, val := it.At()
					if ts < minTS {
						minTS = ts
					}
					if ts > maxTS {
						maxTS = ts
					}

					fmt.Printf("Chunk #%d, sample #%d: ts: %d (%s), val: %g\n", cix, six, ts, formatTimestamp(ts), val)
				case chunkenc.ValHistogram:
					ts, h = it.AtHistogram(h)
					if ts < minTS {
						minTS = ts
					}
					if ts > maxTS {
						maxTS = ts
					}

					fmt.Printf("Chunk #%d, sample #%d: ts: %d (%s), val: %s\n", cix, six, ts, formatTimestamp(ts), h.String())
				case chunkenc.ValFloatHistogram:
					ts, fh = it.AtFloatHistogram(fh)
					if ts < minTS {
						minTS = ts
					}
					if ts > maxTS {
						maxTS = ts
					}

					fmt.Printf("Chunk #%d, sample #%d: ts: %d (%s), val: %s\n", cix, six, ts, formatTimestamp(ts), fh.String())
				default:
					fmt.Printf("Chunk #%d, sample #%d: ts: N/A (N/A), unsupported value type %v", cix, six, valType)
				}
				six++
			}
			if e := it.Err(); e != nil {
				fmt.Printf("Chunk #%d: error: %v\n", cix, e)
			}

			fmt.Printf("Chunk #%d: minTS=%d (%s), maxTS=%d (%s)\n", cix, minTS, formatTimestamp(minTS), maxTS, formatTimestamp(maxTS))
		}

		cix++
	}

	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func formatTimestamp(ts int64) string {
	const RFC3339Millis = "2006-01-02T15:04:05.999Z07:00"

	return model.Time(ts).Time().UTC().Format(RFC3339Millis)
}

/*
┌────────────────────┬───────────────────┬─────────────────────┬────────────────┐
│ data len <uvarint> │ encoding <1 byte> │ data <length bytes> │ CRC32 <4 byte> │
└────────────────────┴───────────────────┴─────────────────────┴────────────────┘
*/
func nextChunk(chunkIndex int, file *os.File) (chunkenc.Chunk, error) {
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get file position: %w", err)
	}

	buf := make([]byte, binary.MaxVarintLen64)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		return nil, fmt.Errorf("reading chunk length: %w", err)
	}

	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, fmt.Errorf("invalid varint length")
	}

	if n == len(buf) {
		// we could handle this, but it can only happen if chunk length was too big --
		// which it cannot really be, since chunk lengths are < 2^32
		return nil, fmt.Errorf("unable to read encoding, no bytes left in buffer")
	}

	enc := chunkenc.Encoding(buf[n])
	n++

	// seek after Data part of chunk
	chunkData := make([]byte, int(length))

	// Copy remaining data from our buffer
	copy(chunkData, buf[n:])
	_, err = io.ReadFull(file, chunkData[len(buf)-n:])
	if err != nil {
		return nil, fmt.Errorf("failed to skip data: %w", err)
	}

	_, err = io.ReadFull(file, buf[0:4])
	if err != nil {
		return nil, fmt.Errorf("failed to read CRC32: %w", err)
	}

	chunk, err := chunkenc.FromData(enc, chunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk: %w", err)
	}

	fmt.Printf("Chunk #%d: position: %d length: %d encoding: %v, crc32: %02x, samples: %d\n", chunkIndex, pos, length, enc, buf[0:4], chunk.NumSamples())
	return chunk, nil
}
