package shorthand

import (
	"strconv"
)

// makeGenericMap turns a map[string]any into a map[any]any, copying over
// all existing items.
func makeGenericMap(m map[string]any) map[any]any {
	cp := make(map[any]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func isMap(v any) bool {
	if _, ok := v.(map[string]any); ok {
		return true
	} else if _, ok := v.(map[any]any); ok {
		return true
	}
	return false
}

func isArray(v any) bool {
	if _, ok := v.([]any); ok {
		return true
	}
	return false
}

func withDefault(wantMap bool, k any, v any) (any, bool) {
	if wantMap && !isMap(v) {
		if _, ok := k.(string); ok {
			return map[string]any{}, true
		} else {
			return map[any]any{}, true
		}
	} else if !wantMap && !isArray(v) {
		return []any{}, true
	}
	return nil, false
}

func applyValue(_ any, op Operation) (any, Error) {
	return op.Value, nil
}

func (d *Document) applyIndex(input any, op Operation) (any, Error) {
	var err error

	// Handle index
	d.skipWhitespace()
	d.buf.Reset()

	// Start by assuming an append (no index)
	index := -1
	appnd := true
	insert := false
	for {
		// Try to parse each char between the [ and ]
		r := d.next()
		if r == -1 || r == ']' {
			break
		}
		d.buf.WriteRune(r)
	}
	if d.buf.Len() > 0 {
		// We have values, so try to parse it as an index!
		s := d.buf.String()
		if s[0] == '^' {
			insert = true
			s = s[1:]
		}
		index, err = strconv.Atoi(s)
		if err != nil {
			return nil, d.error(uint(len(s)), "Cannot convert index to number")
		}
		appnd = false
	}
	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Setting index %d insert:%v append:%v", index, insert, appnd)
	}

	// Change the type to a slice if needed.
	if !isArray(input) {
		input = []any{}
	}

	s := input.([]any)
	if appnd {
		// Append (i.e. index equals length).
		index = len(s)
	} else if index < 0 {
		// Support negative indexes, e.g. `-1` is the last item.
		index = len(s) + index
	}

	// Grow by appending nil until the slice is the right length.
	grew := false
	for len(s) <= index {
		grew = true
		s = append(s, nil)
	}

	// Handle insertion, i.e. shifting items if needed after appending a new
	// item to shift them into (if the index was within the bounds of the
	// original slice).
	if insert {
		if !grew {
			s = append(s, nil)
		}
		for i := len(s) - 2; i >= index; i-- {
			s[i+1] = s[i]
		}
	}

	// Depending on what the next character is, we either set the value or
	// recurse with more indexes or path parts.
	if p := d.peek(); p == -1 {
		if op.Kind == OpDelete {
			s = append(s[:index], s[index+1:]...)
		} else {
			s[index] = op.Value
		}
	} else if p == '[' {
		d.next()
		result, err := d.applyIndex(s[index], op)
		if err != nil {
			return nil, err
		}
		s[index] = result
	} else if p == '.' {
		d.next()
		result, err := d.applyPathPart(s[index], op)
		if err != nil {
			return nil, err
		}
		s[index] = result
	} else {
		panic("unexpected char " + string(p))
	}

	return s, nil
}

func (d *Document) applyPathPart(input any, op Operation) (any, Error) {
	quoted := false
	d.buf.Reset()

	for {
		r := d.next()

		if r == '\\' {
			if d.parseEscape(false, false) {
				continue
			}
		}

		if r == '"' {
			if err := d.parseQuoted(false); err != nil {
				return nil, err
			}
			quoted = true
			continue
		}

		if r == '.' || r == '[' || r == -1 {
			keystr := d.buf.String()
			var key any = keystr
			d.buf.Reset()

			if key == "" && !quoted {
				// Special case: raw value
				if r == '[' {
					// This raw value is an array.
					return d.applyIndex(input, op)
				}
				return op.Value, nil
			}

			if !d.options.ForceStringKeys && !quoted {
				if tmp, ok := coerceValue(keystr, d.options.ForceFloat64Numbers); ok {
					key = tmp
				}
			}

			if d.options.DebugLogger != nil {
				d.options.DebugLogger("Setting key %v", key)
			}

			wantMap := true
			applyFunc := d.applyPathPart
			if r == '[' {
				wantMap = false
				applyFunc = d.applyIndex
			} else if r == -1 {
				applyFunc = applyValue
			}

			if !isMap(input) {
				if def, ok := withDefault(true, key, nil); ok {
					input = def
				}
			}

			if m, ok := input.(map[string]any); ok {
				if s, ok := key.(string); ok {
					if wantMap && op.Kind == OpDelete {
						delete(m, s)
					} else {
						result, err := applyFunc(m[s], op)
						if err != nil {
							return nil, err
						}
						m[s] = result
					}
					break
				} else {
					// Key is not a string, so convert input into a generic
					// map[any]any and process it further below.
					input = makeGenericMap(m)
				}
			}

			if m, ok := input.(map[any]any); ok {
				if wantMap && op.Kind == OpDelete {
					delete(m, key)
				} else {
					v := m[key]
					if def, ok := withDefault(wantMap, key, v); ok {
						v = def
					}
					result, err := applyFunc(v, op)
					if err != nil {
						return nil, err
					}
					m[key] = result
				}
				break
			}

			panic("can't get here")
		}

		d.buf.WriteRune(r)
	}

	return input, nil
}

func (d *Document) applySwap(input any, op Operation) (any, Error) {
	// First, get both left & right values from the input.
	left, okl, err := GetPath(op.Path, input, GetOptions{DebugLogger: d.options.DebugLogger})
	if err != nil {
		return nil, err
	}
	right, okr, err := GetPath(op.Value.(string), input, GetOptions{DebugLogger: d.options.DebugLogger})
	if err != nil {
		return nil, err
	}

	if d.options.DebugLogger != nil {
		d.options.DebugLogger("Swapping (%v, %t) & (%v, %t)", left, okl, right, okr)
	}

	// Set/unset left to value of right
	kind := OpSet
	if !okr {
		kind = OpDelete
	}
	d.expression = op.Path
	d.pos = 0
	input, err = d.applyPathPart(input, Operation{
		Kind:  kind,
		Path:  op.Path,
		Value: right,
	})
	if err != nil {
		return nil, err
	}

	// Set/unset right to value of left
	kind = OpSet
	if !okl {
		kind = OpDelete
	}
	d.expression = op.Value.(string)
	d.pos = 0
	return d.applyPathPart(input, Operation{
		Kind:  kind,
		Path:  op.Value.(string),
		Value: left,
	})
}

func (d *Document) applyOp(input any, op Operation) (any, Error) {
	d.expression = op.Path
	d.pos = 0
	d.buf.Reset()

	switch op.Kind {
	case OpSet, OpDelete:
		return d.applyPathPart(input, op)
	case OpSwap:
		return d.applySwap(input, op)
	}

	return d.applyPathPart(input, op)
}
