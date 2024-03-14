// SPDX-License-Identifier: AGPL-3.0-only
//
// Portions of this file are derived from json-iterator/go (https://github.com/json-iterator/go),
// which is licensed under the MIT License. The specific functions derived from this source are
// noted below in this file.

package querymiddleware

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"unicode/utf16"
	"unsafe"
)

var shardActiveSeriesDecoderMaxBuffSize = os.Getpagesize()

var shardActiveSeriesResponseDecoderPool = sync.Pool{
	New: func() any {
		return &shardActiveSeriesResponseDecoder{
			br:  bufio.NewReaderSize(nil, 4096),
			out: make([]byte, 0, shardActiveSeriesDecoderMaxBuffSize),
		}
	},
}

func borrowShardActiveSeriesResponseDecoder(ctx context.Context, rc io.ReadCloser) *shardActiveSeriesResponseDecoder {
	d := shardActiveSeriesResponseDecoderPool.Get().(*shardActiveSeriesResponseDecoder)
	d.reset(ctx, rc)
	return d
}

func reuseShardActiveSeriesResponseDecoder(d *shardActiveSeriesResponseDecoder) {
	d.reset(context.Background(), nil)
	if cap(d.out) > shardActiveSeriesDecoderMaxBuffSize {
		return
	}
	shardActiveSeriesResponseDecoderPool.Put(d)
}

type shardActiveSeriesResponseDecoder struct {
	ctx            context.Context
	rc             io.ReadCloser
	br             *bufio.Reader
	out            []byte
	readBytesCount int
	foundDataValue bool
	err            error
}

func (d *shardActiveSeriesResponseDecoder) reset(ctx context.Context, rc io.ReadCloser) {
	d.ctx = ctx
	d.rc = rc
	d.br.Reset(rc)
	d.out = d.out[:0]
	d.readBytesCount = 0
	d.foundDataValue = false
	d.err = nil
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

func (d *shardActiveSeriesResponseDecoder) hasDataValue() bool {
	return d.foundDataValue && len(d.out) > 0
}

func (d *shardActiveSeriesResponseDecoder) dataValue() string {
	if !d.foundDataValue {
		return ""
	}
	return unsafe.String(unsafe.SliceData(d.out), len(d.out))
}

func (d *shardActiveSeriesResponseDecoder) decode() error {
	if d.foundDataValue {
		return nil // already decoded
	}

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
				d.readDataValue()
				if d.err != nil {
					return d.err
				}
				return d.ctx.Err()

			case "error":
				d.readErrorValue()
				if d.err != nil {
					return d.err
				}
				return d.ctx.Err()

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
		return fmt.Errorf("decode: expected {, found %c", c)
	}
	return errors.New("expected data field at top level")
}

func (d *shardActiveSeriesResponseDecoder) readDataValue() {
	if c := d.nextToken(); c != ':' {
		d.stickError(fmt.Errorf("readDataValue: expected :, found %c", c))
		return
	}
	c := d.nextToken()
	switch c {
	case '[':
		d.out = d.out[:0]

		inner := 1

		c := d.readByte()
		for d.err == nil {
			switch c {
			case '[':
				inner++
			case ']':
				inner--
				if inner == 0 {
					d.foundDataValue = true
					return
				}
			default:
				d.out = append(d.out, c)
			}
			c = d.readByte()
		}

	default:
		d.stickError(errors.New("expected data field to contain an array"))
	}
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
		d.stickError(fmt.Errorf(`readString: expected ", found %c`, c))
		return ""
	}
	d.out = d.out[:0]

	for d.err == nil {
		c = d.readByte()
		if c == '"' {
			return unsafe.String(
				unsafe.SliceData(d.out), len(d.out),
			)
		}
		if c == '\\' {
			c = d.readByte()
			d.out = d.readEscapedChar(c, d.out)
		} else {
			d.out = append(d.out, c)
		}
	}
	d.stickError(errors.New("readString: unexpected end of input"))
	return ""
}

func (d *shardActiveSeriesResponseDecoder) readErrorValue() {
	if c := d.nextToken(); c != ':' {
		d.stickError(fmt.Errorf("readErrorValue: expected :, found %c", c))
		return
	}
	d.stickError(fmt.Errorf("error in partial response: %s", d.readString()))
}

func (d *shardActiveSeriesResponseDecoder) skipValue() {
	if tk := d.nextToken(); tk != ':' {
		d.stickError(fmt.Errorf("skipValue: expected :, found %c", tk))
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
	inner := 1
	for d.err == nil {
		switch d.readByte() {
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
	d.out = d.out[:0]
}

func (d *shardActiveSeriesResponseDecoder) readByte() byte {
	b, err := d.br.ReadByte()
	if err != nil {
		d.stickError(err)
		return 0
	}
	// Check for context cancellation every 256 bytes.
	d.readBytesCount++
	if d.readBytesCount%256 == 0 {
		if err := d.ctx.Err(); err != nil {
			if cause := context.Cause(d.ctx); cause != nil {
				d.stickError(fmt.Errorf("decoder context cancelled: %w", cause))
			} else {
				d.stickError(err)
			}
			return 0
		}
	}
	return b
}

func (d *shardActiveSeriesResponseDecoder) unreadByte() {
	if err := d.br.UnreadByte(); err != nil {
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
