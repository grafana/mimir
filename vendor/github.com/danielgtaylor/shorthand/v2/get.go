package shorthand

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/danielgtaylor/mexpr"
	"golang.org/x/exp/maps"
)

type GetOptions struct {
	// DebugLogger sets a function to be used for printing out debug information.
	DebugLogger func(format string, a ...interface{})
}

func GetPath(path string, input any, options GetOptions) (any, bool, Error) {
	d := Document{
		expression: path,
		options: ParseOptions{
			DebugLogger: options.DebugLogger,
		},
	}
	result := input
	var ok bool
	var err Error
	for d.pos < uint(len(d.expression)) {
		result, ok, err = d.getPath(result)
		if err != nil {
			return result, ok, err
		}
		if d.peek() == '|' {
			d.next()
		}
	}
	return result, ok, nil
}

func (d *Document) parseUntil(open int, terminators ...rune) (quoted bool, canSlice bool, err Error) {
	d.buf.Reset()
	return d.parseUntilNoReset(open, terminators...)
}

func (d *Document) parseUntilNoReset(open int, terminators ...rune) (quoted bool, canSlice bool, err Error) {
	canSlice = true
outer:
	for {
		p := d.peek()
		if p == -1 {
			break outer
		}

		if p == '[' || p == '{' {
			open++
		} else if p == ']' || p == '}' {
			open--
			if open == 0 {
				break outer
			}
		}

		for _, t := range terminators {
			if p == t {
				break outer
			}
		}

		r := d.next()

		if r == '\\' {
			if d.parseEscape(false, false) {
				canSlice = false
				continue
			}
		}

		if r == '"' {
			if err = d.parseQuoted(false); err != nil {
				return
			}
			quoted = true
			continue
		}

		d.buf.WriteRune(r)
	}

	return
}

func (d *Document) parsePathIndex() (bool, int, int, string, Error) {
	d.skipWhitespace()
	start := d.pos
	_, canSlice, err := d.parseUntil(1, '|')
	if err != nil {
		return false, 0, 0, "", err
	}

	var value string
	if canSlice {
		value = d.expression[start:d.pos]
	} else {
		value = d.buf.String()
	}

	if !d.expect(']') {
		return false, 0, 0, "", d.error(d.pos-start, "expected ']' after index or filter")
	}

	if len(value) > 0 {
		indexes := strings.Split(value, ":")
		if len(indexes) == 1 {
			if index, err := strconv.Atoi(value); err == nil {
				return false, index, index, "", nil
			}
		} else {
			if indexes[0] == "" {
				indexes[0] = "0"
			}
			if startIndex, err := strconv.Atoi(indexes[0]); err == nil {
				if indexes[1] == "" {
					indexes[1] = "-1"
				}
				if stopIndex, err := strconv.Atoi(indexes[1]); err == nil {
					return true, startIndex, stopIndex, "", nil
				}
			}
		}

		if value[0] == '?' {
			value = value[1:]
		}
	}

	return false, 0, 0, value, nil
}

func (d *Document) getFiltered(expr string, input any) (any, Error) {
	ast, err := mexpr.Parse(expr, nil)
	if err != nil {
		// Pos = current - expression - 1 for bracket + error offset.
		// a.b.c[filter is here]
		// current position....^
		return nil, NewError(&d.expression, d.pos-uint(len(expr)+1)+uint(err.Offset()), uint(err.Length()), err.Error())
	}
	interpreter := mexpr.NewInterpreter(ast, mexpr.UnquotedStrings)
	savedPos := d.pos

	if s, ok := input.([]any); ok {
		results := []any{}
		for _, item := range s {
			result, err := interpreter.Run(item)
			if err != nil {
				continue
			}
			if b, ok := result.(bool); ok && b {
				out := item

				var err Error
				d.pos = savedPos
				out, _, err = d.getPath(item)
				if err != nil {
					return nil, err
				}
				results = append(results, out)
			}
		}
		return results, nil
	}
	return nil, nil
}

func (d *Document) getPathIndex(input any) (any, Error) {
	isSlice, startIndex, stopIndex, expr, err := d.parsePathIndex()
	if err != nil {
		return nil, err
	}

	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Getting index %v:%v %v", startIndex, stopIndex, expr)
	}

	if expr != "" {
		return d.getFiltered(expr, input)
	}

	l := 0
	switch t := input.(type) {
	case string:
		l = len(t)
	case []byte:
		l = len(t)
	case []any:
		l = len(t)
	}

	if startIndex < 0 {
		startIndex += l
	}
	if stopIndex < 0 {
		stopIndex += l
	}
	if stopIndex > l-1 {
		stopIndex = l - 1
	}
	if startIndex < 0 || startIndex > l-1 || stopIndex < 0 || startIndex > stopIndex {
		return nil, nil
	}

	switch t := input.(type) {
	case string:
		if !isSlice {
			return string(t[startIndex]), nil
		}
		return t[startIndex : stopIndex+1], nil
	case []byte:
		if !isSlice {
			return t[startIndex], nil
		}
		return t[startIndex : stopIndex+1], nil
	case []any:
		if !isSlice {
			return t[startIndex], nil
		}
		return t[startIndex : stopIndex+1], nil
	}

	return nil, nil
}

func (d *Document) parseGetProp() (any, Error) {
	d.skipWhitespace()
	start := d.pos
	quoted, canSlice, err := d.parseUntil(0, '.', '[', '|', ',', '}', ']')
	if err != nil {
		return nil, err
	}

	var key string
	if canSlice {
		key = d.expression[start:d.pos]
	} else {
		key = d.buf.String()
	}

	if !d.options.ForceStringKeys && !quoted {
		if v, ok := coerceValue(key, false); ok {
			return v, nil
		}
	}

	return key, nil
}

func (d *Document) getProp(input any) (any, bool, Error) {
	key, err := d.parseGetProp()
	if err != nil {
		return nil, false, err
	}

	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Getting key '%v'", key)
	}

	if m, ok := input.(map[string]any); ok {
		if s, ok := key.(string); ok {
			if key == "*" {
				// Special case: wildcard property
				keys := maps.Keys(m)
				sort.Strings(keys)
				values := make([]any, len(m))
				for i, k := range keys {
					values[i] = m[k]
				}
				return values, true, nil
			}

			v, ok := m[s]
			return v, ok, nil
		}
	} else if m, ok := input.(map[any]any); ok {
		if s, ok := key.(string); ok && s == "*" {
			// Special case: wildcard property
			keys := make([]any, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool {
				// This order is not perfect, but crucially it *is* stable.
				return fmt.Sprintf("%v", keys[i]) < fmt.Sprintf("%v", keys[j])
			})
			values := make([]any, len(m))
			for i, k := range keys {
				values[i] = m[k]
			}
			return values, true, nil
		}

		v, ok := m[key]
		return v, ok, nil
	} else {
		if d.options.DebugLogger != nil {
			d.options.DebugLogger("Cannot get key %v from input %v", key, input)
		}
	}

	return nil, false, nil
}

func (d *Document) getPropRecursive(input any) ([]any, Error) {
	key, err := d.parseGetProp()
	if err != nil {
		return nil, err
	}

	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Recursive getting key '%v'", key)
	}

	return d.findPropRecursive(key, input)
}

func (d *Document) findPropRecursive(key, input any) ([]any, Error) {
	results := []any{}

	savedPos := d.pos
	if m, ok := input.(map[string]any); ok {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := m[k]
			if s, ok := key.(string); ok {
				if k == s {
					results = append(results, v)
				}
			}
			d.pos = savedPos
			if tmp, err := d.findPropRecursive(key, v); err == nil {
				results = append(results, tmp...)
			}
		}
	}
	if m, ok := input.(map[any]any); ok {
		for k, v := range m {
			if k == key {
				results = append(results, v)
			}
			d.pos = savedPos
			if tmp, err := d.findPropRecursive(key, v); err == nil {
				results = append(results, tmp...)
			}
		}
	}
	if s, ok := input.([]any); ok {
		for _, v := range s {
			d.pos = savedPos
			if tmp, err := d.findPropRecursive(key, v); err == nil {
				results = append(results, tmp...)
			}
		}
	}

	return results, nil
}

// flatten nested arrays one level. Returns `nil` if the input is not an array.
func (d *Document) flatten(input any) any {
	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Flattening %v", input)
	}
	if s, ok := input.([]any); ok {
		out := make([]any, 0, len(s))

		for _, item := range s {
			if !isArray(item) {
				out = append(out, item)
				continue
			}

			out = append(out, item.([]any)...)
		}
		return out
	}
	return nil
}

func (d *Document) getPath(input any) (any, bool, Error) {
	var err Error
	found := false

outer:
	for {
		switch d.peek() {
		case -1, '|':
			break outer
		case '[':
			d.next()
			if d.peek() == ']' {
				// Special case: flatten one level
				// [[1, 2], 3, [[4]]] => [1, 2, 3, [4]]
				d.next()
				input = d.flatten(input)
				found = true
				continue
			}
			input, err = d.getPathIndex(input)
			if err != nil {
				return nil, false, err
			}
			found = true
		case '.':
			d.next()
			if d.peek() == '.' {
				// Special case: recursive descent property query
				// i.e. find this key recursively through everything
				d.next()
				input, err = d.getPropRecursive(input)
				if err != nil {
					return nil, false, err
				}
			}
			if s, ok := input.([]any); ok {
				// Special case: input is an slice of items, so run the right hand
				// side on every item in the slice.
				var err Error
				var result any
				savedPos := d.pos
				out := make([]any, 0, len(s))

				if len(s) == 0 {
					// We will get `nil`, but still need to move the read head forwards to
					// prevent an infinite loop here.
					d.getPath(nil)
				}

				for i := range s {
					d.pos = savedPos
					result, _, err = d.getPath(s[i])
					if err != nil {
						return nil, false, err
					}
					if result != nil {
						out = append(out, result)
					}
				}

				return out, true, nil
			}
			continue
		case '{':
			d.next()
			input, err = d.getFields(input)
			if err != nil {
				return nil, false, err
			}
			found = true
			continue
		case ',', ']', '}':
			d.next()
		default:
			input, found, err = d.getProp(input)
			if err != nil {
				return nil, false, err
			}
		}
	}

	return input, found, nil
}

func (d *Document) getFields(input any) (any, Error) {
	d.buf.Reset()
	if !isMap(input) {
		return nil, d.error(1, "field selection requires a map, but found %v", input)
	}
	result := map[string]any{}
	key := ""
	open := 1
	var r rune
	d.skipWhitespace()
	for {
		r = d.next()
		if r == '"' {
			if err := d.parseQuoted(true); err != nil {
				return nil, err
			}
			continue
		}
		if r == '\\' {
			if d.parseEscape(false, true) {
				continue
			}
		}
		if r == -1 {
			break
		}
		if r == '[' {
			d.buf.WriteRune(r)
			d.parseUntilNoReset(1, '|')
			continue
		}
		if r == ':' && open <= 1 {
			key = d.buf.String()
			d.buf.Reset()
			d.skipWhitespace()
			continue
		}
		if r == '{' {
			open++
		}
		if r == '}' {
			open--
		}
		if open == 0 || (open == 1 && r == ',') {
			path := d.buf.String()
			var value any
			if key == "" {
				key = path
				if m, ok := input.(map[any]any); ok {
					value = m[key]
				}
				if m, ok := input.(map[string]any); ok {
					value = m[key]
				}
			} else {
				var err Error
				value, _, err = GetPath(path, input, GetOptions{
					DebugLogger: d.options.DebugLogger,
				})
				if err != nil {
					return nil, err
				}
			}
			result[key] = value
			if r == '}' {
				break
			}
			key = ""
			d.buf.Reset()
			d.skipWhitespace()
			continue
		}
		d.buf.WriteRune(r)
	}
	return result, nil
}
