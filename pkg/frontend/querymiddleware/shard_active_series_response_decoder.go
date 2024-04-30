// SPDX-License-Identifier: AGPL-3.0-only
//
// Portions of this file are derived from json-iterator/go (https://github.com/json-iterator/go),
// which is licensed under the MIT License. The specific functions derived from this source are
// noted below in this file.

package querymiddleware

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"unicode/utf16"
	"unsafe"
)

const (
	defaultActiveSeriesChunkMaxBufferSize = 1024 * 1024 // 1MB

	checkContextCancelledBytesInterval = 256
)

var activeSeriesChunkBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, defaultActiveSeriesChunkMaxBufferSize))
	},
}

var shardActiveSeriesResponseDecoderPool = sync.Pool{
	New: func() any {
		return &shardActiveSeriesResponseDecoder{
			br:      bufio.NewReaderSize(nil, 4096),
			strBuff: make([]byte, 0, 256),
		}
	},
}

func borrowShardActiveSeriesResponseDecoder(ctx context.Context, rc io.ReadCloser, streamCh chan<- *bytes.Buffer) *shardActiveSeriesResponseDecoder {
	d := shardActiveSeriesResponseDecoderPool.Get().(*shardActiveSeriesResponseDecoder)
	d.reset(ctx, rc, streamCh)
	return d
}

func reuseShardActiveSeriesResponseDecoder(d *shardActiveSeriesResponseDecoder) {
	d.reset(context.Background(), nil, nil)
	shardActiveSeriesResponseDecoderPool.Put(d)
}

func reuseActiveSeriesDataStreamBuffer(buf *bytes.Buffer) {
	buf.Reset()
	activeSeriesChunkBufferPool.Put(buf)
}

type shardActiveSeriesResponseDecoder struct {
	ctx                context.Context
	rc                 io.ReadCloser
	br                 *bufio.Reader
	strBuff            []byte
	streamCh           chan<- *bytes.Buffer
	readBytesCount     int
	err                error
	chunkBufferMaxSize int
}

func (d *shardActiveSeriesResponseDecoder) reset(ctx context.Context, rc io.ReadCloser, streamCh chan<- *bytes.Buffer) {
	d.br.Reset(rc)

	d.ctx = ctx
	d.rc = rc
	d.streamCh = streamCh
	d.strBuff = d.strBuff[:0]
	d.readBytesCount = 0
	d.err = nil
	d.chunkBufferMaxSize = defaultActiveSeriesChunkMaxBufferSize
}

func (d *shardActiveSeriesResponseDecoder) stickError(err error) {
	if d.err != nil {
		return
	}
	d.err = err
}

func (d *shardActiveSeriesResponseDecoder) close() {
	_, _ = io.Copy(io.Discard, d.rc)
	_ = d.rc.Close()
}

func (d *shardActiveSeriesResponseDecoder) decode() error {
	c := d.nextToken()
	if d.err != nil {
		return d.err
	}
	switch c {
	case '{':
		for d.err == nil {
			k := d.readString()
			switch k {
			case "data":
				d.readData()
				if d.err != nil {
					return d.err
				}
				return nil

			case "error":
				d.readError()
				return d.err

			default:
				d.skipValue()
			}

			c = d.nextToken()
			if c == ',' {
				continue
			} else if c == '}' {
				break
			}
		}

	default:
		return fmt.Errorf("decode: expected '{', found %c", c)
	}
	return errors.New("expected data field at top level")
}

func (d *shardActiveSeriesResponseDecoder) readData() {
	defer func() {
		d.checkContextCanceled()
	}()

	if c := d.nextToken(); c != ':' {
		d.stickError(fmt.Errorf("readData: expected ':', found %c", c))
		return
	}
	switch d.nextToken() {
	case '[':
		return

	default:
		d.stickError(errors.New("expected data field to contain an array"))
	}
}

func (d *shardActiveSeriesResponseDecoder) readError() {
	defer func() {
		d.checkContextCanceled()
	}()

	if c := d.nextToken(); c != ':' {
		d.stickError(fmt.Errorf("readError: expected ':', found %c", c))
		return
	}
	d.stickError(fmt.Errorf("error in partial response: %s", d.readString()))
}

func (d *shardActiveSeriesResponseDecoder) streamData() error {
	firstItem := true
	expectsComma := false

	cb := activeSeriesChunkBufferPool.Get().(*bytes.Buffer)
	for d.err == nil {
		t := d.nextToken()
		switch t {
		case ']':
			if cb.Len() > 0 {
				d.streamCh <- cb
			}
			d.checkContextCanceled()
			return d.err

		case '{':
			if !firstItem {
				cb.WriteByte(',')
			} else {
				firstItem = false
			}
			cb.WriteByte(t)

			d.readObject(cb)
			if d.err != nil {
				return d.err
			}
			expectsComma = true

		case ',':
			if expectsComma {
				expectsComma = false
				break
			}
			d.stickError(errors.New("streamData: unexpected comma"))
			return d.err

		default:
			d.stickError(fmt.Errorf("streamData: expected '{' or ',', found %c", t))
			return d.err
		}

		if cb.Len() >= d.chunkBufferMaxSize {
			d.streamCh <- cb
			cb = activeSeriesChunkBufferPool.Get().(*bytes.Buffer)
			firstItem = true
		}
	}
	d.checkContextCanceled()
	return d.err
}

func (d *shardActiveSeriesResponseDecoder) nextToken() byte {
	for {
		c := d.readByte()
		if d.err != nil {
			return 0
		}
		switch c {
		case ' ', '\n', '\t', '\r':
			continue
		default:
			return c
		}
	}
}

func (d *shardActiveSeriesResponseDecoder) readString() string {
	c := d.nextToken()
	if c != '"' {
		d.stickError(fmt.Errorf(`readString: expected '"', found %c`, c))
		return ""
	}
	d.strBuff = d.strBuff[:0]

	for d.err == nil {
		c = d.readByte()
		if c == '"' {
			return unsafe.String(
				unsafe.SliceData(d.strBuff), len(d.strBuff),
			)
		}
		if c == '\\' {
			c = d.readByte()
			d.strBuff = d.readEscapedChar(c, d.strBuff)
		} else {
			d.strBuff = append(d.strBuff, c)
		}
	}
	d.stickError(errors.New("readString: unexpected end of input"))
	return ""
}

func (d *shardActiveSeriesResponseDecoder) readObject(buf *bytes.Buffer) {
	inner := 1
	for d.err == nil {
		c := d.readByte()
		if buf != nil {
			buf.WriteByte(c)
		}
		switch c {
		case '{':
			inner++
		case '}':
			inner--
			if inner == 0 {
				return
			}
		}
	}
}

func (d *shardActiveSeriesResponseDecoder) skipValue() {
	if tk := d.nextToken(); tk != ':' {
		d.stickError(fmt.Errorf("skipValue: expected ':', found %c", tk))
		return
	}
	switch d.nextToken() {
	case '{': // object
		d.skipObject()

	case '[': // array
		d.skipArray()

	case '"': // string
		d.skipString()

	default: // number, true, false, null
		c := d.nextToken()
		for d.err == nil {
			if c == ',' || c == '}' {
				d.unreadByte()
				break
			}
			c = d.nextToken()
		}
	}
}

func (d *shardActiveSeriesResponseDecoder) skipObject() {
	d.readObject(nil)
}

func (d *shardActiveSeriesResponseDecoder) skipArray() {
	inner := 1
	for d.err == nil {
		switch d.readByte() {
		case '[':
			inner++
		case ']':
			inner--
			if inner == 0 {
				return
			}
		}
	}
}

func (d *shardActiveSeriesResponseDecoder) skipString() {
	d.unreadByte()
	_ = d.readString()
	d.strBuff = d.strBuff[:0]
}

func (d *shardActiveSeriesResponseDecoder) readByte() byte {
	b, err := d.br.ReadByte()
	if err != nil {
		d.stickError(err)
		return 0
	}
	// Check for context cancellation
	d.readBytesCount++
	if d.readBytesCount%checkContextCancelledBytesInterval == 0 {
		d.checkContextCanceled()
	}
	return b
}

func (d *shardActiveSeriesResponseDecoder) unreadByte() {
	if err := d.br.UnreadByte(); err != nil {
		d.stickError(err)
	}
}

func (d *shardActiveSeriesResponseDecoder) checkContextCanceled() {
	if err := d.ctx.Err(); err != nil {
		if cause := context.Cause(d.ctx); cause != nil {
			d.stickError(fmt.Errorf("context canceled: %w", cause))
			return
		}
		d.stickError(err)
	}
}

// Originally from json-iterator/go (https://github.com/json-iterator/go/blob/71ac16282d122fdd1e3a6d3e7f79b79b4cc3b50e/iter_str.go#L54)
func (d *shardActiveSeriesResponseDecoder) readEscapedChar(c byte, str []byte) []byte {
	switch c {
	case 'u':
		r := d.readU4()
		if utf16.IsSurrogate(r) {
			c = d.readByte()
			if d.err != nil {
				return nil
			}
			if c != '\\' {
				d.unreadByte()
				str = appendRune(str, r)
				return str
			}
			c = d.readByte()
			if d.err != nil {
				return nil
			}
			if c != 'u' {
				str = appendRune(str, r)
				return d.readEscapedChar(c, str)
			}
			r2 := d.readU4()
			if d.err != nil {
				return nil
			}
			combined := utf16.DecodeRune(r, r2)
			if combined == '\uFFFD' {
				str = appendRune(str, r)
				str = appendRune(str, r2)
			} else {
				str = appendRune(str, combined)
			}
		} else {
			str = appendRune(str, r)
		}
	case '"':
		str = append(str, '"')
	case '\\':
		str = append(str, '\\')
	case '/':
		str = append(str, '/')
	case 'b':
		str = append(str, '\b')
	case 'f':
		str = append(str, '\f')
	case 'n':
		str = append(str, '\n')
	case 'r':
		str = append(str, '\r')
	case 't':
		str = append(str, '\t')
	default:
		d.stickError(errors.New(`readEscapedChar: invalid escape char after \`))
		return nil
	}
	return str
}

// Originally from json-iterator/go (https://github.com/json-iterator/go/blob/71ac16282d122fdd1e3a6d3e7f79b79b4cc3b50e/iter_str.go#L146)
func (d *shardActiveSeriesResponseDecoder) readU4() (ret rune) {
	for i := 0; i < 4; i++ {
		c := d.readByte()
		if d.err != nil {
			return
		}
		if c >= '0' && c <= '9' {
			ret = ret*16 + rune(c-'0')
		} else if c >= 'a' && c <= 'f' {
			ret = ret*16 + rune(c-'a'+10)
		} else if c >= 'A' && c <= 'F' {
			ret = ret*16 + rune(c-'A'+10)
		} else {
			d.stickError(errors.New("readU4: expects 0~9 or a~f, but found " + string([]byte{c})))
			return
		}
	}
	return ret
}

const (
	t1 = 0x00 // 0000 0000
	tx = 0x80 // 1000 0000
	t2 = 0xC0 // 1100 0000
	t3 = 0xE0 // 1110 0000
	t4 = 0xF0 // 1111 0000
	t5 = 0xF8 // 1111 1000

	maskx = 0x3F // 0011 1111
	mask2 = 0x1F // 0001 1111
	mask3 = 0x0F // 0000 1111
	mask4 = 0x07 // 0000 0111

	rune1Max = 1<<7 - 1
	rune2Max = 1<<11 - 1
	rune3Max = 1<<16 - 1

	surrogateMin = 0xD800
	surrogateMax = 0xDFFF

	maxRune   = '\U0010FFFF' // Maximum valid Unicode code point.
	runeError = '\uFFFD'     // the "error" Rune or "Unicode replacement character"
)

// Originally from json-iterator/go (https://github.com/json-iterator/go/blob/71ac16282d122fdd1e3a6d3e7f79b79b4cc3b50e/iter_str.go#L190)
func appendRune(p []byte, r rune) []byte {
	// Negative values are erroneous. Making it unsigned addresses the problem.
	switch i := uint32(r); {
	case i <= rune1Max:
		p = append(p, byte(r))
		return p
	case i <= rune2Max:
		p = append(p, t2|byte(r>>6))
		p = append(p, tx|byte(r)&maskx)
		return p
	case i > maxRune, surrogateMin <= i && i <= surrogateMax:
		r = runeError
		fallthrough
	case i <= rune3Max:
		p = append(p, t3|byte(r>>12))
		p = append(p, tx|byte(r>>6)&maskx)
		p = append(p, tx|byte(r)&maskx)
		return p
	default:
		p = append(p, t4|byte(r>>18))
		p = append(p, tx|byte(r>>12)&maskx)
		p = append(p, tx|byte(r>>6)&maskx)
		p = append(p, tx|byte(r)&maskx)
		return p
	}
}
