// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	//"math"

	"github.com/dennwc/varint"
	"github.com/pkg/errors"
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

type Reader interface {
	Reset()
	ResetAt(off int) error
	Read(int) []byte
	Peek(int) []byte
	Len() int

	// TODO: Seems like we need a "Remaining()" method here because Decbuf
	//  expects to be able to do `d.B = d.B[n:]` when it consumes parts of the
	//  underlying byte slice.

	// TODO: Add Seek method here to allow us to skip bytes without
	//  needing to read them or allocate? Easy to implement for BufReader
	//  and supported by bufio.Reader used in FileReader via Discard()
}

type BufReader struct {
	initial []byte
	b       []byte
}

func NewBufReader(bs ByteSlice) *BufReader {
	b := bs.Range(0, bs.Len())
	r := &BufReader{initial: b}
	r.Reset()
	return r
}

func (b *BufReader) Reset() {
	b.b = b.initial
}

func (b *BufReader) ResetAt(off int) error {
	b.b = b.initial
	if len(b.b) < off {
		return ErrInvalidSize
	}
	b.b = b.b[off:]
	return nil
}

func (b *BufReader) Peek(n int) []byte {
	if len(b.b) < n {
		n = len(b.b)
	}
	res := b.b[:n]
	return res
}

func (b *BufReader) Read(n int) []byte {
	if len(b.b) < n {
		n = len(b.b)
	}
	res := b.b[:n]
	b.b = b.b[n:]
	return res
}

func (b *BufReader) Len() int {
	return len(b.b)
}

type FileReader struct {
	file   *os.File
	buf    *bufio.Reader
	base   int
	length int
}

func NewFileReader(file *os.File, base, length int) *FileReader {
	f := &FileReader{
		file:   file,
		buf:    bufio.NewReader(file),
		base:   base,
		length: length,
	}
	f.Reset()
	return f
}

func (f *FileReader) Reset() {
	if _, err := f.file.Seek(int64(f.base), io.SeekStart); err != nil {
		fmt.Printf("seek: %v\n", err)
	}
	f.buf.Reset(f.file)
}

func (f *FileReader) ResetAt(off int) error {
	if _, err := f.file.Seek(int64(f.base+off), io.SeekStart); err != nil {
		fmt.Printf("seek: %v\n", err)
	}
	f.buf.Reset(f.file)
	return nil
}

func (f *FileReader) Peek(n int) []byte {
	b, err := f.buf.Peek(n)
	fmt.Printf("peek: %v\n", err)
	if len(b) > 0 {
		return b
	}
	return nil
}

func (f *FileReader) Read(n int) []byte {
	b := make([]byte, n)
	n, err := f.buf.Read(b)
	fmt.Printf("read: %v\n", err)
	if n > 0 {
		return b[:n]
	}
	return nil
}

func (f *FileReader) Len() int {
	// TODO: BufReader returns whatever is left in the backing byte slice
	//  here while _we_ only ever return the original size of the file (instead
	//  of how much of the file we haven't yet read). Is this a problem? Seems
	//  like it will be.
	return f.length
}

// Decbuf provides safe methods to extract data from a byte slice. It does all
// necessary bounds checking and advancing of the byte slice.
// Several datums can be extracted without checking for errors. However, before using
// any datum, the err() method must be checked.
type Decbuf struct {
	//	B []byte
	r Reader
	E error
}

func NewDecbufAt(bs ByteSlice, off int, castagnoliTable *crc32.Table) Decbuf {
	return NewDecbuf(NewBufReader(bs), off, castagnoliTable)
}

// NewDecbufAt returns a new decoding buffer. It expects the first 4 bytes
// after offset to hold the big endian encoded content length, followed by the contents and the expected
// checksum.
func NewDecbuf(r Reader, off int, castagnoliTable *crc32.Table) Decbuf {
	if err := r.ResetAt(off); err != nil {
		return Decbuf{E: err}
	}
	lenBytes := r.Read(4)
	if len(lenBytes) != 4 {
		return Decbuf{E: ErrInvalidSize}
	}
	l := int(binary.BigEndian.Uint32(lenBytes))

	//	crcBytes := r.Read(4)
	//	if len

	//	if bs.Len() < off+4+l+4 {
	//		return Decbuf{E: ErrInvalidSize}
	//	}

	// Load bytes holding the contents plus a CRC32 checksum.
	//	b = bs.Range(off+4, off+4+l+4)
	//	dec := Decbuf{B: b[:len(b)-4]}

	if castagnoliTable != nil {

		data := r.Read(l)
		if len(data) != l {
			return Decbuf{E: ErrInvalidSize}
		}

		crcBytes := r.Read(4)
		if len(crcBytes) != 4 {
			return Decbuf{E: ErrInvalidSize}
		}

		exp := binary.BigEndian.Uint32(crcBytes)
		res := crc32.Checksum(data, castagnoliTable)
		if exp != res {
			return Decbuf{E: ErrInvalidChecksum}
		}

		if err := r.ResetAt(off); err != nil {
			return Decbuf{E: err}
		}
		_ = r.Read(4)
	}
	//	return dec
	return Decbuf{r: r}
}

// NewDecbufUvarintAt returns a new decoding buffer. It expects the first bytes
// after offset to hold the uvarint-encoded buffers length, followed by the contents and the expected
// checksum.
/*
func NewDecbufUvarintAt(bs ByteSlice, off int, castagnoliTable *crc32.Table) Decbuf {
	// We never have to access this method at the far end of the byte slice. Thus just checking
	// against the MaxVarintLen32 is sufficient.
	if bs.Len() < off+binary.MaxVarintLen32 {
		return Decbuf{E: ErrInvalidSize}
	}
	b := bs.Range(off, off+binary.MaxVarintLen32)

	l, n := varint.Uvarint(b)
	if n <= 0 || n > binary.MaxVarintLen32 {
		return Decbuf{E: errors.Errorf("invalid uvarint %d", n)}
	}

	if bs.Len() < off+n+int(l)+4 {
		return Decbuf{E: ErrInvalidSize}
	}

	// Load bytes holding the contents plus a CRC32 checksum.
	b = bs.Range(off+n, off+n+int(l)+4)
	dec := Decbuf{B: b[:len(b)-4]}

	if dec.Crc32(castagnoliTable) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return Decbuf{E: ErrInvalidChecksum}
	}
	return dec
}
*/
func NewDecbufRaw(bs ByteSlice) Decbuf {
	b := bs.Range(0, bs.Len())
	r := &BufReader{initial: b}
	r.Reset()
	return Decbuf{r: r}
}

func NewDecbufRawReader(r Reader) Decbuf {
	r.Reset()
	return Decbuf{r: r}
}

/*
// NewDecbufRaw returns a new decoding buffer of the given length.
func NewDecbufRaw(bs ByteSlice, length int) Decbuf {
	if bs.Len() < length {
		return Decbuf{E: ErrInvalidSize}
	}
	return Decbuf{B: bs.Range(0, length)}
}
*/
func (d *Decbuf) Uvarint() int { return int(d.Uvarint64()) }
func (d *Decbuf) Be32int() int { return int(d.Be32()) }

//func (d *Decbuf) Be64int64() int64 { return int64(d.Be64()) }

// Crc32 returns a CRC32 checksum over the remaining bytes.
//func (d *Decbuf) Crc32(castagnoliTable *crc32.Table) uint32 {
//	return crc32.Checksum(d.B, castagnoliTable)
//}

func (d *Decbuf) Skip(l int) {
	b := d.r.Peek(l)
	if len(b) < l {
		d.E = ErrInvalidSize
		return
	}
	_ = d.r.Read(l)
}

func (d *Decbuf) UvarintStr() string {
	return string(d.UvarintBytes())
}

// The return value becomes invalid if the byte slice goes away.
// Compared to UvarintStr, this avoid allocations.
func (d *Decbuf) UvarintBytes() []byte {
	//fmt.Printf("UvarintBytes\n")
	l := d.Uvarint64()
	if d.E != nil {
		return []byte{}
	}
	//fmt.Printf("UvarintBytes l=%d\n", l)
	b := d.r.Read(int(l))

	//fmt.Printf("UvarintBytes read=%d\n", len(b))
	if len(b) < int(l) {
		d.E = ErrInvalidSize
		return []byte{}
	}
	//fmt.Printf("UvarintBytes ok\n")
	return b
	//	if len(d.B) < int(l) {
	//		d.E = ErrInvalidSize
	//		return []byte{}
	//	}
	//	s := d.B[:l]
	//	d.B = d.B[l:]
	//	return s
}

/*
func (d *Decbuf) Varint64() int64 {
	if d.E != nil {
		return 0
	}
	// Decode as unsigned first, since that's what the varint library implements.
	ux, n := varint.Uvarint(d.B)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	// Now decode "ZigZag encoding" https://developers.google.com/protocol-buffers/docs/encoding#signed_integers.
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	d.B = d.B[n:]
	return x
}
*/
func (d *Decbuf) Uvarint64() uint64 {
	//fmt.Printf("Uvarint64\n")
	if d.E != nil {
		return 0
	}
	b := d.r.Peek(10)
	//fmt.Printf("Uvarint64 peeked=%d\n", len(b))
	x, n := varint.Uvarint(b)
	//	x, n := varint.Uvarint(d.B)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	//fmt.Printf("Uvarint64 consume=%d\n", n)
	_ = d.r.Read(n)
	//d.B = d.B[n:]
	return x
}

/*
func (d *Decbuf) Be64() uint64 {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 8 {
		d.E = ErrInvalidSize
		return 0
	}
	x := binary.BigEndian.Uint64(d.B)
	d.B = d.B[8:]
	return x
}

func (d *Decbuf) Be64Float64() float64 {
	return math.Float64frombits(d.Be64())
}
*/
func (d *Decbuf) Be32() uint32 {
	if d.E != nil {
		return 0
	}
	//	if len(d.B) < 4 {
	//		d.E = ErrInvalidSize
	//		return 0
	//	}
	b := d.r.Read(4)
	if len(b) != 4 {
		d.E = ErrInvalidSize
		return 0
	}
	return binary.BigEndian.Uint32(b)

	//x := binary.BigEndian.Uint32(d.B)
	//d.B = d.B[4:]
	//return x
}

/*
func (d *Decbuf) Byte() byte {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	x := d.B[0]
	d.B = d.B[1:]
	return x
}

func (d *Decbuf) ConsumePadding() {
	if d.E != nil {
		return
	}
	for len(d.B) > 1 && d.B[0] == '\x00' {
		d.B = d.B[1:]
	}
	if len(d.B) < 1 {
		d.E = ErrInvalidSize
	}
}
*/
func (d *Decbuf) Err() error { return d.E }
func (d *Decbuf) Len() int   { return d.r.Len() }

//func (d *Decbuf) Get() []byte { return d.B }

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}
