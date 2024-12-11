package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"

	"github.com/prometheus/prometheus/model/histogram"
)

type ChunkSeries struct {
	Labels []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"labels"`
	Chunks []struct {
		StartTimestampMs string `json:"startTimestampMs"`
		EndTimestampMs   string `json:"endTimestampMs"`
		Encoding         int    `json:"encoding"`
		Data             string `json:"data"`
	} `json:"chunks"`
}

type Response struct {
	ChunkSeries []ChunkSeries `json:"chunkseries"`
}

func printBinary(name string, num uint64) {
	fmt.Printf("%s: %064b\n", name, num)
}

func parseChunk(data string) error {
	// Decode base64 data
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return fmt.Errorf("failed to decode base64: %v", err)
	}

	iterator := &xorIterator{
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		br:       newBReader(decoded[2:]),
		numTotal: binary.BigEndian.Uint16(decoded),
		t:        math.MinInt64,
	}

	for iterator.Next() != ValNone {
		t, v := iterator.At()
		fmt.Println("t:", t, "v:", v)
	}

	return nil
}

func main() {
	// Read JSON file

	for _, file := range []string{
		"/Users/dimitar/grafana/mimir/tools/grpcurl-query-ingesters/chunks-dump/ingester-zone-a-78",
		"/Users/dimitar/grafana/mimir/tools/grpcurl-query-ingesters/chunks-dump/ingester-zone-b-78",
		"/Users/dimitar/grafana/mimir/tools/grpcurl-query-ingesters/chunks-dump/ingester-zone-c-78",
	} {
		fmt.Println("Processing", file)
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read file: %v", err)
		}
		parseIngesterResponse(data)
	}
}

func parseIngesterResponse(data []byte) {
	// Parse JSON
	var response Response
	if err := json.Unmarshal(data, &response); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}

	// Process each chunk series
	for i, series := range response.ChunkSeries {
		fmt.Printf("Series %d:\n", i+1)

		// Decode and print labels
		for _, label := range series.Labels {
			_, _ = base64.StdEncoding.DecodeString(label.Name)
			_, _ = base64.StdEncoding.DecodeString(label.Value)
			//fmt.Printf("Label: %s = %s\n", name, value)
		}

		// Process each chunk
		for j, chunk := range series.Chunks {
			fmt.Printf("\nChunk %d (Start: %s, End: %s):\n",
				j+1, chunk.StartTimestampMs, chunk.EndTimestampMs)

			if err := parseChunk(chunk.Data); err != nil {
				log.Printf("Error parsing chunk %d: %v", j+1, err)
				continue
			}
		}
		fmt.Println()
	}
}

type xorIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t   int64
	val uint64

	leading  uint8
	trailing uint8

	tDelta uint64
	err    error
}

func (it *xorIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValFloat
}

func (it *xorIterator) At() (int64, uint64) {
	return it.t, it.val
}

func (it *xorIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call xorIterator.AtHistogram")
}

func (it *xorIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call xorIterator.AtFloatHistogram")
}

func (it *xorIterator) AtT() int64 {
	return it.t
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b)

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.leading = 0
	it.trailing = 0
	it.tDelta = 0
	it.err = nil
}

func (it *xorIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal {
		return ValNone
	}

	if it.numRead == 0 {
		t, err := binary.ReadVarint(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.t = t
		it.val = v

		it.numRead++
		return ValFloat
	}
	if it.numRead == 1 {
		tDelta, err := binary.ReadUvarint(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.tDelta = tDelta
		it.t += int64(it.tDelta)

		return it.readValue()
	}

	var d byte
	// read delta-of-delta
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBitFast()
		if err != nil {
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return ValNone
		}
		if bit == zero {
			break
		}
		d |= 1
	}
	var sz uint8
	var dod int64
	switch d {
	case 0b0:
		// dod == 0
	case 0b10:
		sz = 14
	case 0b110:
		sz = 17
	case 0b1110:
		sz = 20
	case 0b1111:
		// Do not use fast because it's very unlikely it will succeed.
		bits, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return ValNone
		}

		dod = int64(bits)
	}

	if sz != 0 {
		bits, err := it.br.readBitsFast(sz)
		if err != nil {
			bits, err = it.br.readBits(sz)
		}
		if err != nil {
			it.err = err
			return ValNone
		}

		// Account for negative numbers, which come back as high unsigned numbers.
		// See docs/bstream.md.
		if bits > (1 << (sz - 1)) {
			bits -= 1 << sz
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod)
	it.t += int64(it.tDelta)

	return it.readValue()
}

func (it *xorIterator) readValue() ValueType {
	err := xorRead(&it.br, &it.val, &it.leading, &it.trailing)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.numRead++
	return ValFloat
}

// bstream is a stream of bits.
type bstream struct {
	stream []byte // The data stream.
	count  uint8  // How many right-most bits are available for writing in the current byte (the last byte of the stream).
}

// Reset resets b around stream.
func (b *bstream) Reset(stream []byte) {
	b.stream = stream
	b.count = 0
}

func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, byt)
		return
	}

	i := len(b.stream) - 1

	// Complete the last byte with the leftmost b.count bits from byt.
	b.stream[i] |= byt >> (8 - b.count)

	// Write the remainder, if any.
	b.stream = append(b.stream, byt<<b.count)
}

// writeBits writes the nbits right-most bits of u to the stream
// in left-to-right order.
func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= 64 - uint(nbits)
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

type bstreamReader struct {
	stream       []byte
	streamOffset int // The offset from which read the next byte from the stream.

	buffer uint64 // The current buffer, filled from the stream, containing up to 8 bytes from which read bits.
	valid  uint8  // The number of right-most bits valid to read (from left) in the current 8 byte buffer.
	last   byte   // A copy of the last byte of the stream.
}

func newBReader(b []byte) bstreamReader {
	// The last byte of the stream can be updated later, so we take a copy.
	var last byte
	if len(b) > 0 {
		last = b[len(b)-1]
	}
	return bstreamReader{
		stream: b,
		last:   last,
	}
}

func (b *bstreamReader) readBit() (bit, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(1) {
			return false, io.EOF
		}
	}

	return b.readBitFast()
}

// readBitFast is like readBit but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBit().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitFast() (bit, error) {
	if b.valid == 0 {
		return false, io.EOF
	}

	b.valid--
	bitmask := uint64(1) << b.valid
	return (b.buffer & bitmask) != 0, nil
}

// readBits constructs a uint64 with the nbits right-most bits
// read from the stream, and any other bits 0.
func (b *bstreamReader) readBits(nbits uint8) (uint64, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}

	if nbits <= b.valid {
		return b.readBitsFast(nbits)
	}

	// We have to read all remaining valid bits from the current buffer and a part from the next one.
	bitmask := (uint64(1) << b.valid) - 1
	nbits -= b.valid
	v := (b.buffer & bitmask) << nbits
	b.valid = 0

	if !b.loadNextBuffer(nbits) {
		return 0, io.EOF
	}

	bitmask = (uint64(1) << nbits) - 1
	v |= ((b.buffer >> (b.valid - nbits)) & bitmask)
	b.valid -= nbits

	return v, nil
}

// readBitsFast is like readBits but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBits().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > b.valid {
		return 0, io.EOF
	}

	bitmask := (uint64(1) << nbits) - 1
	b.valid -= nbits

	return (b.buffer >> b.valid) & bitmask, nil
}

func (b *bstreamReader) ReadByte() (byte, error) {
	v, err := b.readBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// loadNextBuffer loads the next bytes from the stream into the internal buffer.
// The input nbits is the minimum number of bits that must be read, but the implementation
// can read more (if possible) to improve performances.
func (b *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if b.streamOffset >= len(b.stream) {
		return false
	}

	// Handle the case there are more then 8 bytes in the buffer (most common case)
	// in a optimized way. It's guaranteed that this branch will never read from the
	// very last byte of the stream (which suffers race conditions due to concurrent
	// writes).
	if b.streamOffset+8 < len(b.stream) {
		b.buffer = binary.BigEndian.Uint64(b.stream[b.streamOffset:])
		b.streamOffset += 8
		b.valid = 64
		return true
	}

	// We're here if there are 8 or less bytes left in the stream.
	// The following code is slower but called less frequently.
	nbytes := int((nbits / 8) + 1)
	if b.streamOffset+nbytes > len(b.stream) {
		nbytes = len(b.stream) - b.streamOffset
	}

	buffer := uint64(0)
	skip := 0
	if b.streamOffset+nbytes == len(b.stream) {
		// There can be concurrent writes happening on the very last byte
		// of the stream, so use the copy we took at initialization time.
		buffer |= uint64(b.last)
		// Read up to the byte before
		skip = 1
	}

	for i := 0; i < nbytes-skip; i++ {
		buffer |= (uint64(b.stream[b.streamOffset+i]) << uint(8*(nbytes-i-1)))
	}

	b.buffer = buffer
	b.streamOffset += nbytes
	b.valid = uint8(nbytes * 8)

	return true
}

func xorRead(br *bstreamReader, value *uint64, leading, trailing *uint8) error {
	bit, err := br.readBitFast()
	if err != nil {
		bit, err = br.readBit()
	}
	if err != nil {
		return err
	}
	if bit == zero {
		return nil
	}
	bit, err = br.readBitFast()
	if err != nil {
		bit, err = br.readBit()
	}
	if err != nil {
		return err
	}

	var (
		bits                           uint64
		newLeading, newTrailing, mbits uint8
	)

	if bit == zero {
		// Reuse leading/trailing zero bits.
		newLeading, newTrailing = *leading, *trailing
		mbits = 64 - newLeading - newTrailing
	} else {
		bits, err = br.readBitsFast(5)
		if err != nil {
			bits, err = br.readBits(5)
		}
		if err != nil {
			return err
		}
		newLeading = uint8(bits)

		bits, err = br.readBitsFast(6)
		if err != nil {
			bits, err = br.readBits(6)
		}
		if err != nil {
			return err
		}
		mbits = uint8(bits)
		// 0 significant bits here means we overflowed and we actually
		// need 64; see comment in xrWrite.
		if mbits == 0 {
			mbits = 64
		}
		newTrailing = 64 - newLeading - mbits
		// Update leading/trailing zero bits for the caller.
		*leading, *trailing = newLeading, newTrailing
	}
	bits, err = br.readBitsFast(mbits)
	if err != nil {
		bits, err = br.readBits(mbits)
	}
	if err != nil {
		return err
	}
	vbits := *value
	vbits ^= bits << newTrailing
	*value = vbits
	return nil
}

// ValueType defines the type of a value an Iterator points to.
type ValueType uint8

// Possible values for ValueType.
const (
	ValNone           ValueType = iota // No value at the current position.
	ValFloat                           // A simple float, retrieved with At.
	ValHistogram                       // A histogram, retrieve with AtHistogram, but AtFloatHistogram works, too.
	ValFloatHistogram                  // A floating-point histogram, retrieve with AtFloatHistogram.
)

func (v ValueType) String() string {
	switch v {
	case ValNone:
		return "none"
	case ValFloat:
		return "float"
	case ValHistogram:
		return "histogram"
	case ValFloatHistogram:
		return "floathistogram"
	default:
		return "unknown"
	}
}

func (v ValueType) ChunkEncoding() Encoding {
	switch v {
	case ValFloat:
		return EncXOR
	case ValHistogram:
		return EncHistogram
	case ValFloatHistogram:
		return EncFloatHistogram
	default:
		return EncNone
	}
}

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncXOR
	EncHistogram
	EncFloatHistogram
)

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	case EncHistogram:
		return "histogram"
	case EncFloatHistogram:
		return "floathistogram"
	}
	return "<unknown>"
}

// IsValidEncoding returns true for supported encodings.
func IsValidEncoding(e Encoding) bool {
	return e == EncXOR || e == EncHistogram || e == EncFloatHistogram
}
