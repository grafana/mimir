// Package casing helps convert between CamelCase, snake_case, and others. It
// includes an intelligent `Split` function to take almost any input and then
// convert it to any type of casing.
package casing

import (
	"strconv"
	"strings"
	"unicode"
)

// Borrowed from Go lint (https://github.com/golang/lint)
var commonInitialisms = map[string]bool{
	"ACL":   true,
	"API":   true,
	"ASCII": true,
	"CPU":   true,
	"CSS":   true,
	"DNS":   true,
	"EOF":   true,
	"GUID":  true,
	"HTML":  true,
	"HTTP":  true,
	"HTTPS": true,
	"ID":    true,
	"IP":    true,
	"JSON":  true,
	"LHS":   true,
	"QPS":   true,
	"RAM":   true,
	"RHS":   true,
	"RPC":   true,
	"SLA":   true,
	"SMTP":  true,
	"SQL":   true,
	"SSH":   true,
	"TCP":   true,
	"TLS":   true,
	"TTL":   true,
	"UDP":   true,
	"UI":    true,
	"UID":   true,
	"UUID":  true,
	"URI":   true,
	"URL":   true,
	"UTF8":  true,
	"VM":    true,
	"XML":   true,
	"XMPP":  true,
	"XSRF":  true,
	"XSS":   true,

	// Media initialisms
	"1080P":  true,
	"2D":     true,
	"3D":     true,
	"4K":     true,
	"8K":     true,
	"AAC":    true,
	"AC3":    true,
	"CDN":    true,
	"DASH":   true,
	"DRM":    true,
	"DVR":    true,
	"EAC3":   true,
	"FPS":    true,
	"GOP":    true,
	"H264":   true,
	"H265":   true,
	"HD":     true,
	"HLS":    true,
	"MJPEG":  true,
	"MP2T":   true,
	"MP3":    true,
	"MP4":    true,
	"MPEG2":  true,
	"MPEG4":  true,
	"NTSC":   true,
	"PCM":    true,
	"RGB":    true,
	"RGBA":   true,
	"RTMP":   true,
	"RTP":    true,
	"SCTE":   true,
	"SCTE35": true,
	"SMPTE":  true,
	"UPID":   true,
	"UPIDS":  true,
	"VOD":    true,
	"YUV420": true,
	"YUV422": true,
	"YUV444": true,
}

var commonSuffixes = map[string]bool{
	// E.g. 2D, 3D
	"D": true,
	// E.g. 100GB
	"GB": true,
	// E.g. 4K, 8K
	"K": true,
	// E.g. 100KB
	"KB": true,
	// E.g. 64kbps
	"KBPS": true,
	// E.g. 100MB
	"MB": true,
	// E.g. 2500mbps
	"MPBS": true,
	// E.g. 1080P
	"P": true,
	// E.g. 100TB
	"TB": true,
}

// TransformFunc is used to transform parts of a split string during joining.
type TransformFunc func(string) string

// Identity is the identity transformation function.
func Identity(part string) string {
	return part
}

// Initialism converts common initialisms like ID and HTTP to uppercase.
func Initialism(part string) string {
	if u := strings.ToUpper(part); commonInitialisms[u] {
		return u
	}
	return part
}

// State machine for splitting.
const (
	stateNone       = 0
	stateLower      = 1
	stateFirstUpper = 2
	stateUpper      = 3
	stateSymbol     = 4
)

// Split a value intelligently, taking into account different casing styles,
// numbers, symbols, etc.
//
//    // Returns: ["HTTP", "Server", "2020"]
//    casing.Split("HTTPServer_2020")
func Split(value string) []string {
	results := []string{}
	start := 0
	state := stateNone

	for i, c := range value {
		// Regardless of state, these always break words. Handles kabob and snake
		// casing, respectively.
		if unicode.IsSpace(c) || unicode.IsPunct(c) {
			if i-start > 0 {
				results = append(results, value[start:i])
			}
			start = i + 1
			state = stateNone
			continue
		}

		switch {
		case state != stateFirstUpper && state != stateUpper && unicode.IsUpper(c):
			// Initial uppercase, might start a word, e.g. Camel
			if start != i {
				results = append(results, value[start:i])
				start = i
			}
			state = stateFirstUpper
		case state == stateFirstUpper && unicode.IsUpper(c):
			// String of uppercase to be grouped, e.g. HTTP
			state = stateUpper
		case state != stateSymbol && !unicode.IsLetter(c):
			// Anything -> non-letter
			if start != i {
				results = append(results, value[start:i])
				start = i
			}
			state = stateSymbol
		case state != stateLower && unicode.IsLower(c):
			if state == stateUpper {
				// Multi-character uppercase to lowercase. Last item in the uppercase
				// is part of the lowercase string, e.g. HTTPServer.
				if i > 0 && start != i-1 {
					results = append(results, value[start:i-1])
					start = i - 1
				}
			} else if state != stateFirstUpper {
				// End of a non-uppercase or non-lowercase string. Ignore the first
				// upper state as it's part of the same word.
				if i > 0 && start != i {
					results = append(results, value[start:i])
					start = i
				}
			}
			state = stateLower
		}
	}

	// Include whatever is at the end of the string.
	if start < len(value) {
		results = append(results, value[start:])
	}

	return results
}

// Join will combine split parts back together with the given separator and
// optional transform functions.
func Join(parts []string, sep string, transform ...TransformFunc) string {
	for i := 0; i < len(parts); i++ {
		for _, t := range transform {
			parts[i] = t(parts[i])

			if parts[i] == "" {
				// Transformer completely removed this part.
				parts = append(parts[:i], parts[i+1:]...)
				i--
			}
		}
	}

	return strings.Join(parts, sep)
}

// MergeNumbers will merge some number parts with their adjacent letter parts
// to support a smarter delimited join. For example, `h264` instead of `h_264`
// or `mp3-player` instead of `mp-3-player`. You can pass suffixes for right
// aligning certain items, e.g. `K` to get `MODE_4K` instead of `MODE4_K`. If
// no suffixes are passed, then a default set of common ones is used. Pass an
// empty string to disable the default.
func MergeNumbers(parts []string, suffixes ...string) []string {
	// TODO: should we do this in-place instead?
	results := make([]string, 0, len(parts))
	prevNum := false

	suffixLookup := map[string]bool{}
	for _, word := range suffixes {
		suffixLookup[strings.ToUpper(word)] = true
	}
	if len(suffixes) == 0 {
		suffixLookup = commonSuffixes
	}

	for i := 0; i < len(parts); i++ {
		part := parts[i]
		if _, err := strconv.Atoi(part); err == nil {
			// This part is a number!

			// Special case: right aligned word
			if i < len(parts)-1 && suffixLookup[strings.ToUpper(parts[i+1])] {
				results = append(results, part+parts[i+1])
				i++
				continue
			}

			if !prevNum {
				if i == 0 {
					// First item must always append.
					results = append(results, part)
				} else {
					// Concatenate the number to the previous non-number piece.
					results[len(results)-1] += part
				}
				prevNum = true
				continue
			}

			prevNum = true
		} else {
			// Special case: first part is a number, second part is not.
			if i == 1 && prevNum {
				results[0] += part
				prevNum = false
				continue
			}

			prevNum = false
		}

		results = append(results, part)
	}

	return results
}

// Camel returns a CamelCase version of the input. If no transformation
// functions are passed in, then strings.ToLower is used. This can be disabled
// by passing in the identity function.
func Camel(value string, transform ...TransformFunc) string {
	if transform == nil {
		transform = []TransformFunc{strings.ToLower}
	}
	t := append(transform, strings.Title)
	return Join(Split(value), "", t...)
}

// LowerCamel returns a lowerCamelCase version of the input.
func LowerCamel(value string, transform ...TransformFunc) string {
	runes := []rune(Camel(value, transform...))
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

// Snake returns a snake_case version of the input.
func Snake(value string, transform ...TransformFunc) string {
	if transform == nil {
		transform = []TransformFunc{strings.ToLower}
	}
	return Join(MergeNumbers(Split(value)), "_", transform...)
}

// Kebab returns a kabob-case version of the input.
func Kebab(value string, transform ...TransformFunc) string {
	if transform == nil {
		transform = []TransformFunc{strings.ToLower}
	}
	return Join(MergeNumbers(Split(value)), "-", transform...)
}
