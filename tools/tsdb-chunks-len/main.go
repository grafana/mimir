package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"
)

type indexTOC struct {
	SeriesOffset, LabelIndex1 int64
}

func main() {
	const indexFileObjectPath = "9960/01F4X9CCYRQ2M9EFDNN72S6EPG/index"
	bkt := createBucketClient()

	indexFileObjectSize, err := objectSize(bkt, indexFileObjectPath)
	noErr(err)
	indexFileTOC, err := readIndexTOC(bkt, indexFileObjectPath, indexFileObjectSize)
	noErr(err)
	fmt.Printf("got TOC; series offset %d; series length %d\n", indexFileTOC.SeriesOffset, indexFileTOC.LabelIndex1-indexFileTOC.SeriesOffset)

	indexReader, err := bkt.GetRange(context.Background(), indexFileObjectPath, indexFileTOC.SeriesOffset, indexFileTOC.LabelIndex1-indexFileTOC.SeriesOffset)
	noErr(err)
	defer indexReader.Close()

	seriesCh := make(chan series)
	go readChunkRefs(indexReader, seriesCh)

	start := time.Now()
	doChunkRangeStats(seriesCh)
	fmt.Println(time.Since(start))
}

func doChunkRangeStats(seriesCh <-chan series) {
	var lastChunk chunks.Meta
	lastSeriesMaxChunkLen := -1

	for s := range seriesCh {
		maxChunklen := 0
		prevChunkLen := uint64(0)
		for refIdx, ref := range s.refs {
			if lastSeriesMaxChunkLen != -1 || refIdx != 0 { // if we aren't on the first chunk of the first series
				seq := uint(id >> 32)

				prevChunkLen = uint64(lastChunk.Ref - ref.Ref)
			}
			if lastSeriesMaxChunkLen != -1 && refIdx == 0 { // this is a chunk of a new series, and it isn't the first chunk of the first series
				fmt.Printf("%d %f\n", prevChunkLen, float64(prevChunkLen)/float64(lastSeriesMaxChunkLen))
			}
			if refIdx < len(s.refs) // we want to fidn out the lenth of the last chunk relative to the
		}
	}
}

var crcHasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))

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
	reader := &trackedReader{r: bufio.NewReader(r)}
	var crcBytes [crc32.Size]byte

	for numSeries := 0; ; numSeries++ {
		if numSeries%10000 == 0 {
			fmt.Printf("series %d offset %d\n", numSeries, reader.off)
		}
		// series are 16-byte aligned; we need to skip until the next series
		if remainder := int64(reader.off % 16); remainder != 0 {
			_, err := io.CopyN(io.Discard, reader, 16-remainder)

			if errors.Is(err, io.EOF) {
				close(seriesCh)
				return
			}
		}

		// Read the length of the series (doesn't include the length fo the crc at the end)
		seriesBytesLen, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			close(seriesCh)
			return
		}

		seriesStartOffset := reader.off
		noErr(err)
		//fmt.Printf("series %d id %d len %d ", numSeries, seriesStartOffset, seriesBytesLen)

		wholeSeriesBytes := make([]byte, seriesBytesLen)
		_, err = io.ReadFull(reader, wholeSeriesBytes)
		noErr(err)

		chks, err := decodeSeriesForTime(wholeSeriesBytes)
		noErr(err)

		//numLabels, err := binary.ReadUvarint(reader)
		//noErr(err)
		//fmt.Printf("num labels %d ", numLabels)
		//for i := uint64(0); i < numLabels; i++ {
		//	_, err = binary.ReadUvarint(reader) // label name
		//	noErr(err)
		//	_, err = binary.ReadUvarint(reader) // label val
		//	noErr(err)
		//}
		//
		//numChunks, err := binary.ReadUvarint(reader)
		//noErr(err)
		//fmt.Printf("num chunks %d\n", numChunks)
		//for i := uint64(0); i < numChunks; i++ {
		//	_, err = binary.ReadUvarint(reader) // ref
		//	noErr(err)
		//	_, err = binary.ReadUvarint(reader) // mint
		//	noErr(err)
		//	_, err = binary.ReadUvarint(reader) // maxt
		//	noErr(err)
		//}

		// Some sanity check that we haven't messed up the reading
		if uint64(reader.off-seriesStartOffset) != seriesBytesLen {
			panic(fmt.Sprintf("not correct %d != %d", reader.off-seriesStartOffset, seriesBytesLen))
		}

		crcHasher.Reset()
		_, _ = crcHasher.Write(wholeSeriesBytes)

		_, err = io.ReadFull(reader, crcBytes[:]) // crc32
		noErr(err)

		if binary.BigEndian.Uint32(crcBytes[:]) != crcHasher.Sum32() {
			panic("crc doesn't match")
		}

		seriesCh <- series{refs: chks}
	}

}
func decodeSeriesForTime(b []byte) (chks []chunks.Meta, err error) {
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
