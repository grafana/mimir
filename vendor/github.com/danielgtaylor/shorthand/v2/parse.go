package shorthand

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/fxamacker/cbor/v2"
)

var JSONReplacements = map[rune]rune{
	'"':  '"',
	'\\': '\\',
	'/':  '/',
	'b':  '\b',
	'f':  '\f',
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
}

// runeStr returns a rune as a string, taking care to handle -1 as end-of-file.
func runeStr(r rune) string {
	if r == -1 {
		return "EOF"
	} else if r == '\n' {
		return "\\n"
	}
	return string(r)
}

func canCoerce(value string) bool {
	if value == "null" {
		return true
	} else if value == "true" {
		return true
	} else if value == "false" {
		return true
	} else if len(value) >= 10 && value[0] >= '0' && value[0] <= '9' && value[3] >= '0' && value[3] <= '9' && value[4] == '-' && value[7] == '-' {
		return true
	} else if len(value) > 0 && ((value[0] >= '0' && value[0] <= '9') || value[0] == '-' || value[0] == '+' || value[0] == '.') {
		return true
	}
	return false
}

func coerceValue(value string, forceFloat bool) (any, bool) {
	if value == "null" {
		return nil, true
	} else if value == "true" {
		return true, true
	} else if value == "false" {
		return false, true
	} else if len(value) >= 10 && value[0] >= '0' && value[0] <= '9' && value[3] >= '0' && value[3] <= '9' && value[4] == '-' && value[7] == '-' {
		// This looks date or time-like.
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return t, true
		}
	} else if len(value) > 0 && ((value[0] >= '0' && value[0] <= '9') || value[0] == '-' || value[0] == '+' || value[0] == '.') {
		// This looks like a number.
		isFloat := false
		for _, r := range value {
			if r == '.' || r == 'e' || r == 'E' {
				isFloat = true
				break
			}
		}
		if isFloat || forceFloat {
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				return f, true
			}
		} else if i, err := strconv.Atoi(value); err == nil {
			return i, true
		}
	}
	return nil, false
}

// next returns the next rune in the expression at the current position.
func (d *Document) next() rune {
	if d.pos >= uint(len(d.expression)) {
		d.lastWidth = 0
		return -1
	}

	var r rune
	if d.expression[d.pos] < utf8.RuneSelf {
		// Optimization for a simple ASCII character
		r = rune(d.expression[d.pos])
		d.pos += 1
		d.lastWidth = 1
	} else {
		var w int
		r, w = utf8.DecodeRuneInString(d.expression[d.pos:])
		d.pos += uint(w)
		d.lastWidth = uint(w)
	}

	return r
}

// Back moves back one rune.
func (d *Document) back() {
	d.pos -= d.lastWidth
}

// peek returns the next rune without moving the position forward.
func (d *Document) peek() rune {
	if d.pos >= uint(len(d.expression)) {
		return -1
	}

	var r rune
	if d.expression[d.pos] < utf8.RuneSelf {
		// Optimization for a simple ASCII character
		r = rune(d.expression[d.pos])
	} else {
		r, _ = utf8.DecodeRuneInString(d.expression[d.pos:])
	}

	return r
}

// expect returns true if the next value is the given value, otherwise false.
// ignores whitespace.
func (d *Document) expect(value rune) bool {
	d.skipWhitespace()
	peek := d.peek()
	if peek == value {
		d.next()
		return true
	}
	return false
}

func (d *Document) error(length uint, format string, a ...any) Error {
	return NewError(&d.expression, d.pos, length, format, a...)
}

func (d *Document) skipWhitespace() {
	for {
		peek := d.peek()
		if unicode.IsSpace(peek) {
			d.next()
			continue
		}
		break
	}
}

func (d *Document) skipComments(r rune) bool {
	if r == '/' && d.peek() == '/' {
		for {
			r = d.next()
			if r == -1 || r == '\n' {
				break
			}
		}
		d.skipWhitespace()
		return true
	}
	return false
}

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
// This is taken from the official Go JSON decoder.
func getu4(s []byte) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	var r rune
	for _, c := range s[2:6] {
		switch {
		case '0' <= c && c <= '9':
			c = c - '0'
		case 'a' <= c && c <= 'f':
			c = c - 'a' + 10
		case 'A' <= c && c <= 'F':
			c = c - 'A' + 10
		default:
			return -1
		}
		r = r*16 + rune(c)
	}
	return r
}

func (d *Document) parseEscape(quoted bool, includeEscape bool) bool {
	peek := d.peek()
	if !quoted {
		if peek == '.' || peek == '{' || peek == '[' || peek == ':' || peek == '^' || peek == ']' || peek == ',' {
			d.next()
			if includeEscape {
				d.buf.WriteRune('\\')
			}
			d.buf.WriteRune(peek)
			return true
		}
	} else {
		if peek == '"' {
			d.next()
			if includeEscape {
				d.buf.WriteRune('\\')
			}
			d.buf.WriteRune(peek)
			return true
		}
	}

	if replace, ok := JSONReplacements[peek]; ok {
		d.next()
		d.buf.WriteRune(replace)
		return true
	}
	if (peek == 'u' || peek == 'U') && len(d.expression) >= int(d.pos)+5 {
		r := getu4([]byte(d.expression[d.pos-1:]))
		if r >= 0 {
			b := make([]byte, 4)
			d.pos += 5 // We already consumed the '\'
			if utf16.IsSurrogate(r) {
				// This is a two character UTF16 sequence as two '\uXXXX' pairs.
				r2 := getu4([]byte(d.expression[d.pos:]))
				if dec := utf16.DecodeRune(r, r2); dec != unicode.ReplacementChar {
					d.pos += 6
					w := utf8.EncodeRune(b, dec)
					d.buf.Write(b[:w])
					return true
				}
				// Invalid surrogate!
				r = unicode.ReplacementChar
			}
			// Otherwise: this is a normal single '\uXXXX' encoded character.
			w := utf8.EncodeRune(b, r)
			d.buf.Write(b[:w])
			return true
		}
	}

	return false
}

func (d *Document) parseQuoted(escapeProp bool) Error {
	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Parsing quoted string")
	}
	start := d.pos
	for {
		r := d.next()
		if r == '\\' {
			if d.parseEscape(true, escapeProp) {
				continue
			}
		}

		if escapeProp {
			if r == '.' || r == '{' || r == '[' || r == ':' || r == '^' {
				d.buf.WriteRune('\\')
			}
		}

		if r == -1 {
			return NewError(&d.expression, start, d.pos-start, "Expected quote but found EOF")
		} else if r == '"' {
			break
		} else {
			d.buf.WriteRune(r)
		}
	}
	return nil
}

func (d *Document) parseIndex() Error {
	for {
		r := d.next()

		if (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '^' {
			d.buf.WriteRune(r)
			continue
		}

		d.back()
		break
	}

	if d.expect(']') {
		d.buf.WriteRune(']')
	} else {
		return d.error(1, "Expected ']' but found %s", runeStr(d.next()))
	}

	return nil
}

func (d *Document) parseProp(path string, commaStop bool) (string, Error) {
	start := d.pos
	d.skipWhitespace()
	d.buf.Reset()

	for {
		r := d.next()

		if r == '[' {
			d.buf.WriteRune(r)
			if err := d.parseIndex(); err != nil {
				return "", err
			}
			continue
		}

		if r == -1 || r == ':' || r == '{' || r == '}' || r == '^' || r == ']' || (commaStop && r == ',') {
			d.back()
			break
		}

		if r == '"' {
			if err := d.parseQuoted(true); err != nil {
				return "", err
			}
			prop := d.buf.String()

			if canCoerce(prop) || prop == "" {
				// This could be coerced into another type, so let's keep it wrapped
				// in quotes to ensure it is treated properly.
				prop = `"` + prop + `"`
			}

			if path != "" {
				return path + "." + prop, nil
			}
			return prop, nil
		}

		if r == '\\' {
			if d.parseEscape(false, true) {
				continue
			}
		}

		if d.skipComments(r) {
			continue
		}

		d.buf.WriteRune(r)
	}

	var prop string
	if path != "" {
		prop = path + "." + strings.TrimSpace(d.buf.String())
	} else {
		prop = strings.TrimSpace(d.buf.String())
	}

	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Setting key %s", prop)
	}

	if prop == "" {
		return "", d.error(d.pos-start, "expected at least one property name")
	}

	return prop, nil
}

func (d *Document) parseObject(path string) Error {
	// Special case: empty object
	d.skipWhitespace()
	if d.peek() == '}' {
		d.Operations = append(d.Operations, Operation{
			Kind:  OpSet,
			Path:  path,
			Value: map[string]any{},
		})
	}

	for {
		d.skipWhitespace()
		r := d.peek()

		if r == -1 || r == '}' {
			break
		}

		if r == ',' {
			d.next()
			continue
		}

		prop, err := d.parseProp(path, false)
		if err != nil {
			return err
		}
		r = d.next()
		if r == ']' {
			// Common error: incorrect order of closing backets/braces.
			d.back()
			return d.error(1, "Expected property or '}' while parsing object but got ']'")
		} else if r == '{' {
			// a{b: 1} is equivalent to a: {b: 1}, so we just send this to be parsed
			// as a value.
			d.back()
		} else if r == '^' {
			// a ^ b is a swap operation which takes a fully-qualified path as its
			// value. The result of the paths are swapped in the resulting structure.
			v, err := d.parseProp("", true)
			if err != nil {
				return err
			}
			d.Operations = append(d.Operations, Operation{
				Kind:  OpSwap,
				Path:  prop,
				Value: v,
			})
			continue
		} else {
			if r != ':' {
				return d.error(1, "Expected colon but got %v", runeStr(r))
			}
		}
		if err := d.parseValue(prop, true, true); err != nil {
			return err
		}
		if strings.Contains(path, "[]") {
			// Subsequent paths should not append additional values.
			path = strings.ReplaceAll(path, "[]", "[-1]")
		}
	}
	return nil
}

func (d *Document) parseValue(path string, coerce bool, terminateComma bool) Error {
	d.skipWhitespace()
	d.buf.Reset()
	start := d.pos
	canSlice := true
	first := true

	for {
		r := d.next()

		if d.skipComments(r) {
			canSlice = false
			continue
		}

		if r == '\\' {
			if d.parseEscape(false, false) {
				canSlice = false
				first = false
				continue
			}
		}

		if first {
			if r == '{' {
				if d.options.DebugLogger != nil {
					d.options.DebugLogger("Parsing sub-object")
				}
				start = d.pos
				if err := d.parseObject(path); err != nil {
					return err
				}
				if d.options.DebugLogger != nil {
					d.options.DebugLogger("Sub-object done")
				}
				if !d.expect('}') {
					return d.error(d.pos-start, "Expected '}' but found %s", runeStr(r))
				}
				d.skipWhitespace()
				d.skipComments(d.peek())
				break
			} else if r == '[' {
				if d.options.DebugLogger != nil {
					d.options.DebugLogger("Parsing sub-array")
				}
				// Special case: empty array
				d.skipWhitespace()
				if d.peek() == ']' {
					if d.options.DebugLogger != nil {
						d.options.DebugLogger("Parse value: []")
					}
					d.Operations = append(d.Operations, Operation{
						Kind:  OpSet,
						Path:  path,
						Value: []any{},
					})
					d.next()
					break
				}

				idx := 0
				for {
					if idx > 0 && strings.Contains(path, "[]") {
						path = strings.ReplaceAll(path, "[]", "[-1]")
					}
					if err := d.parseValue(path+"["+strconv.Itoa(idx)+"]", true, true); err != nil {
						return err
					}

					d.skipWhitespace()
					peek := d.peek()
					if peek == ']' {
						d.next()
						break
					} else if peek == ',' {
						d.next()
					} else {
						return d.error(1, "Expected ',' or ']' but found '%s'", runeStr(peek))
					}

					idx++
				}
				if d.options.DebugLogger != nil {
					d.options.DebugLogger("Sub-array done")
				}
				break
			} else if r == '"' {
				if err := d.parseQuoted(false); err != nil {
					return err
				}
				if d.options.DebugLogger != nil {
					d.options.DebugLogger("Parse value: %v", d.buf.String())
				}
				d.Operations = append(d.Operations, Operation{
					Kind:  OpSet,
					Path:  path,
					Value: d.buf.String(),
				})
				break
			}
		}
		first = false

		if r == -1 || r == '\n' || r == '}' || r == ']' || (terminateComma && r == ',') {
			if r == '\n' {
				d.skipWhitespace()
			} else {
				d.back()
			}
			var value string
			if canSlice {
				value = strings.TrimSpace(d.expression[start:d.pos])
			} else {
				value = strings.TrimSpace(d.buf.String())
			}

			if coerce && len(value) > 0 {
				if d.options.EnableFileInput && strings.HasPrefix(value, "@") && len(value) > 1 {
					filename := value[1:]

					if d.options.DebugLogger != nil {
						d.options.DebugLogger("Found file %s", filename)
					}

					data, err := os.ReadFile(filename)
					if err != nil {
						return d.error(uint(len(value)), "Unable to read file: %v", err)
					}

					if strings.HasSuffix(filename, ".json") {
						var structured any
						if err := json.Unmarshal(data, &structured); err != nil {
							return d.error(uint(len(value)), "Unable to unmarshal JSON: %v", err)
						}
						if d.options.DebugLogger != nil {
							d.options.DebugLogger("Parse value: %v", structured)
						}
						d.Operations = append(d.Operations, Operation{
							Kind:  OpSet,
							Path:  path,
							Value: structured,
						})
						break
					} else if strings.HasSuffix(filename, ".cbor") {
						var structured any
						if err := cbor.Unmarshal(data, &structured); err != nil {
							return d.error(uint(len(value)), "Unable to unmarshal CBOR: %v", err)
						}

						if d.options.ForceStringKeys {
							structured = ConvertMapString(structured)
						}
						if d.options.DebugLogger != nil {
							d.options.DebugLogger("Parse value: %v", structured)
						}
						d.Operations = append(d.Operations, Operation{
							Kind:  OpSet,
							Path:  path,
							Value: structured,
						})
						break
					} else if utf8.Valid(data) {
						value = string(data)
					} else {
						if d.options.DebugLogger != nil {
							d.options.DebugLogger("Parse value: %v", data)
						}
						d.Operations = append(d.Operations, Operation{
							Kind:  OpSet,
							Path:  path,
							Value: data,
						})
						break
					}
				} else if strings.HasPrefix(value, "%") {
					binary, err := base64.StdEncoding.DecodeString(value[1:])
					if err != nil {
						return d.error(uint(len(value)), "Unable to Base64 decode: %v", err)
					}
					if d.options.DebugLogger != nil {
						d.options.DebugLogger("Parse value: %v", binary)
					}
					d.Operations = append(d.Operations, Operation{
						Kind:  OpSet,
						Path:  path,
						Value: binary,
					})
					break
				} else {
					if value == "undefined" {
						if d.options.DebugLogger != nil {
							d.options.DebugLogger("Unsetting value")
						}
						d.Operations = append(d.Operations, Operation{
							Kind: OpDelete,
							Path: path,
						})
						break
					}

					if coerced, ok := coerceValue(value, d.options.ForceFloat64Numbers); ok {
						if d.options.DebugLogger != nil {
							d.options.DebugLogger("Parse value: %v", coerced)
						}
						d.Operations = append(d.Operations, Operation{
							Kind:  OpSet,
							Path:  path,
							Value: coerced,
						})
						break
					}
				}
			}

			if d.options.DebugLogger != nil {
				d.options.DebugLogger("Parse value: " + value)
			}
			d.Operations = append(d.Operations, Operation{
				Kind:  OpSet,
				Path:  path,
				Value: value,
			})
			break
		}

		d.buf.WriteRune(r)
	}
	return nil
}
