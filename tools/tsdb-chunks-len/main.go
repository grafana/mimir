package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	os "os"
	"os/signal"
	"runtime/debug"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"

	util_math "github.com/grafana/mimir/pkg/util/math"
)

type indexTOC struct {
	SeriesOffset, LabelIndex1 int64
}

func main() {
	var indexFileObjectPath = os.Args[1]
	bkt := createBucketClient()

	ctx, cancel := context.WithCancel(context.Background())

	indexFileObjectSize, err := objectSize(bkt, indexFileObjectPath)
	noErr(err)
	indexFileTOC, err := readIndexTOC(bkt, indexFileObjectPath, indexFileObjectSize)
	noErr(err)

	indexReader, err := bkt.GetRange(ctx, indexFileObjectPath, indexFileTOC.SeriesOffset, indexFileTOC.LabelIndex1-indexFileTOC.SeriesOffset)
	noErr(err)
	defer indexReader.Close()

	seriesCh := make(chan series)
	go readChunkRefs(indexReader, seriesCh)
	go listenForSignals(cancel)

	start := time.Now()
	doChunkRangeStats(seriesCh)
	fmt.Println(time.Since(start))
}

func listenForSignals(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	<-sigs
	cancel()
}

func doChunkRangeStats(seriesCh <-chan series) {
	var lastChunk chunks.Meta
	prevSeriesMaxChunkLen := -1
	var prevSeries series

	for s := range seriesCh {
		prevChunkLen := 0
		maxChunkLen := 0

		for cIdx, chunk := range s.refs {
			if prevSeriesMaxChunkLen == -1 && cIdx == 0 {
				lastChunk = chunk
				continue
			}

			if chunkSegmentFile(chunk.Ref) == chunkSegmentFile(lastChunk.Ref) {
				prevChunkLen = int(chunk.Ref - lastChunk.Ref)
			} else {
				prevChunkLen = math.MaxInt // 1024 * 1024 * 1024B == 1 GiB; unrealistic, so we can spot these in the output
				fmt.Printf("next segment file\n")
				if cIdx != 0 {
					fmt.Printf("one series with chunks in multiple segment files\n")
				}
			}

			if cIdx == 0 {
				// This is a chunk of a new series, we can record how big the last chunk of the last series was
				if len(prevSeries.refs) == 1 { // We can only record it if the previous series had more than one chunk
					fmt.Printf("one chunk\n")
				} else if prevChunkLen != math.MaxInt { // And when the chunks of this series and the previous aren't in two different segment files
					fmt.Printf("%d %f\n", prevChunkLen, float64(prevChunkLen)/float64(prevSeriesMaxChunkLen))
				}
			}
			if cIdx > 0 {
				maxChunkLen = util_math.Max(maxChunkLen, prevChunkLen)
			}
			lastChunk = chunk
		}

		prevSeriesMaxChunkLen = maxChunkLen
		prevSeries = s
	}
}

var crcHasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))

func chunkSegmentFile(id chunks.ChunkRef) int { return int(id >> 32) }
func chunkOffset(id chunks.ChunkRef) uint32   { return uint32(id) }

type trackedReader struct {
	off int
	r   interface {
		io.Reader
		io.ByteReader
	}
}

func (t *trackedReader) ReadByte() (byte, error) {
	t.off++
	return t.r.ReadByte()
}

func (t *trackedReader) Read(p []byte) (n int, err error) {
	n, err = t.r.Read(p)
	t.off += n
	return
}

type series struct {
	refs []chunks.Meta
}

func readChunkRefs(r io.Reader, seriesCh chan<- series) {
	defer close(seriesCh)
	reader := &trackedReader{r: bufio.NewReader(r)}
	var crcBytes [crc32.Size]byte

	for {
		//if numSeries%10000 == 0 {
		//	fmt.Printf("series %d offset %d\n", numSeries, reader.off)
		//}
		// series are 16-byte aligned; we need to skip until the next series
		if remainder := int64(reader.off % 16); remainder != 0 {
			_, err := io.CopyN(io.Discard, reader, 16-remainder)

			if errors.Is(err, io.EOF) {
				return
			}
		}

		// Read the length of the series (doesn't include the length fo the crc at the end)
		seriesBytesLen, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if checkErr(err) {
			return
		}
		seriesStartOffset := reader.off

		wholeSeriesBytes := make([]byte, seriesBytesLen)
		_, err = io.ReadFull(reader, wholeSeriesBytes)
		if checkErr(err) {
			return
		}

		chks, err := decodeSeries(wholeSeriesBytes)
		if checkErr(err) {
			return
		}

		// Some sanity check that we haven't messed up the reading
		if uint64(reader.off-seriesStartOffset) != seriesBytesLen {
			panic(fmt.Sprintf("not correct %d != %d", reader.off-seriesStartOffset, seriesBytesLen))
		}

		crcHasher.Reset()
		_, _ = crcHasher.Write(wholeSeriesBytes)

		_, err = io.ReadFull(reader, crcBytes[:])
		if checkErr(err) {
			return
		}

		if binary.BigEndian.Uint32(crcBytes[:]) != crcHasher.Sum32() {
			panic("crc doesn't match")
		}

		seriesCh <- series{refs: chks}
	}

}

func checkErr(err error) bool {
	if err != nil {
		fmt.Printf("err %s\n%s\n", err, string(debug.Stack()))
		return true
	}
	return false
}

func decodeSeries(b []byte) (chks []chunks.Meta, err error) {
	d := encoding.Decbuf{B: b}

	// Read labels without looking up symbols.
	k := d.Uvarint()
	for i := 0; i < k; i++ {
		_ = d.Uvarint() // label name
		_ = d.Uvarint() // label value
	}
	// Read the chunks meta data.
	k = d.Uvarint()
	if k == 0 {
		return nil, d.Err()
	}

	// First t0 is absolute, rest is just diff so different type is used (Uvarint64).
	mint := d.Varint64()
	maxt := int64(d.Uvarint64()) + mint
	// Similar for first ref.
	ref := int64(d.Uvarint64())

	for i := 0; i < k; i++ {
		if i > 0 {
			mint += int64(d.Uvarint64())
			maxt = int64(d.Uvarint64()) + mint
			ref += d.Varint64()
		}

		chks = append(chks, chunks.Meta{
			Ref:     chunks.ChunkRef(ref),
			MinTime: mint,
			MaxTime: maxt,
		})

		mint = maxt
	}
	return chks, d.Err()
}

func createBucketClient() objstore.BucketReader {
	bkt, err := gcs.NewBucketWithConfig(context.Background(), log.NewLogfmtLogger(os.Stdout), gcs.Config{
		Bucket:         "dev-us-central1-cortex-tsdb-dev",
		ServiceAccount: "", // This will be injected via GOOGLE_APPLICATION_CREDENTIALS
	}, "some bucket")
	noErr(err)
	return bkt
}

func readIndexTOC(bkt objstore.BucketReader, path string, size int64) (indexTOC, error) {
	const TOCSize = 52
	r, err := bkt.GetRange(context.Background(), path, size-TOCSize, TOCSize)
	if err != nil {
		return indexTOC{}, errors.Wrap(err, "reading index file TOC")
	}
	defer r.Close()

	TOCSlice := make([]byte, TOCSize)
	n, err := io.ReadFull(r, TOCSlice)
	if err != nil {
		return indexTOC{}, errors.Wrapf(err, "reading series offset, read %d", n)
	}
	if n != TOCSize {
		panic(fmt.Sprintf("didn't read %d bytes, read %d instead", TOCSize, n))
	}

	decoder := encoding.NewDecbufRaw(realByteSlice(TOCSlice), len(TOCSlice))
	symbolsOffset := decoder.Be64() // Symbols table offset
	seriesOffset := decoder.Be64()
	labelIndex1Offset := decoder.Be64()

	if symbolsOffset == 0 || seriesOffset == 0 || labelIndex1Offset == 0 {
		panic("seriesOffset, labelIndex1Offset, or symbolsOffset is zero")
	}

	if seriesOffset%16 != 0 {
		// The series offset may not be 16-byte aligned even though each series must be 16-byte aligned.
		seriesOffset += 16 - (seriesOffset % 16)
	}

	return indexTOC{
		SeriesOffset: int64(seriesOffset),
		LabelIndex1:  int64(labelIndex1Offset),
	}, nil
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}
func noErr(err error) {
	if err != nil {
		panic(err)
	}
}

func objectSize(bkt objstore.BucketReader, path string) (int64, error) {
	attr, err := bkt.Attributes(context.Background(), path)
	return attr.Size, errors.Wrap(err, "reading file size")
}
