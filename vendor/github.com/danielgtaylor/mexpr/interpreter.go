package mexpr

import (
	"math"
	"strings"

	"golang.org/x/exp/maps"
)

// InterpreterOption passes configuration settings when creating a new
// interpreter instance.
type InterpreterOption int

const (
	// StrictMode does extra checks like making sure identifiers exist.
	StrictMode InterpreterOption = iota

	// UnqoutedStrings enables the use of unquoted string values rather than
	// returning nil or a missing identifier error. Identifiers get priority
	// over unquoted strings.
	UnquotedStrings
)

// checkBounds returns an error if the index is out of bounds.
func checkBounds(ast *Node, input any, idx int) Error {
	if v, ok := input.([]any); ok {
		if idx < 0 || idx >= len(v) {
			return NewError(ast.Offset, ast.Length, "invalid index %d for slice of length %d", int(idx), len(v))
		}
	}
	if v, ok := input.(string); ok {
		if idx < 0 || idx >= len(v) {
			return NewError(ast.Offset, ast.Length, "invalid index %d for string of length %d", int(idx), len(v))
		}
	}
	return nil
}

// Interpreter executes expression AST programs.
type Interpreter interface {
	Run(value any) (any, Error)
}

// NewInterpreter returns an interpreter for the given AST.
func NewInterpreter(ast *Node, options ...InterpreterOption) Interpreter {
	strict := false
	unquoted := false

	for _, opt := range options {
		switch opt {
		case StrictMode:
			strict = true
		case UnquotedStrings:
			unquoted = true
		}
	}

	return &interpreter{
		ast:      ast,
		strict:   strict,
		unquoted: unquoted,
	}
}

type interpreter struct {
	ast             *Node
	prevFieldSelect bool
	strict          bool
	unquoted        bool
}

func (i *interpreter) Run(value any) (any, Error) {
	return i.run(i.ast, value)
}

func (i *interpreter) run(ast *Node, value any) (any, Error) {
	if ast == nil {
		return nil, nil
	}

	fromSelect := i.prevFieldSelect
	i.prevFieldSelect = false

	switch ast.Type {
	case NodeIdentifier:
		switch ast.Value.(string) {
		case "@":
			return value, nil
		case "length":
			// Special pseudo-property to get the value's length.
			if s, ok := value.(string); ok {
				return len(s), nil
			}
			if a, ok := value.([]any); ok {
				return len(a), nil
			}
		case "lower":
			if s, ok := value.(string); ok {
				return strings.ToLower(s), nil
			}
		case "upper":
			if s, ok := value.(string); ok {
				return strings.ToUpper(s), nil
			}
		}
		if m, ok := value.(map[string]any); ok {
			if v, ok := m[ast.Value.(string)]; ok {
				return v, nil
			}
		}
		if m, ok := value.(map[any]any); ok {
			if v, ok := m[ast.Value]; ok {
				return v, nil
			}
		}
		if i.unquoted && !fromSelect {
			// Identifiers not found in the map are treated as strings, but only if
			// the previous item was not a `.` like `obj.field`.
			return ast.Value.(string), nil
		}
		if !i.strict {
			return nil, nil
		}
		return nil, NewError(ast.Offset, ast.Length, "cannot get %v from %v", ast.Value, value)
	case NodeFieldSelect:
		i.prevFieldSelect = true
		leftValue, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		i.prevFieldSelect = true
		return i.run(ast.Right, leftValue)
	case NodeArrayIndex:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		if !isSlice(resultLeft) && !isString(resultLeft) {
			return nil, NewError(ast.Offset, ast.Length, "can only index strings or arrays but got %v", resultLeft)
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		if isSlice(resultRight) && len(resultRight.([]any)) == 2 {
			start, err := toNumber(ast, resultRight.([]any)[0])
			if err != nil {
				return nil, err
			}
			end, err := toNumber(ast, resultRight.([]any)[1])
			if err != nil {
				return nil, err
			}
			if left, ok := resultLeft.([]any); ok {
				if start < 0 {
					start += float64(len(left))
				}
				if end < 0 {
					end += float64(len(left))
				}
				if err := checkBounds(ast, left, int(start)); err != nil {
					return nil, err
				}
				if err := checkBounds(ast, left, int(end)); err != nil {
					return nil, err
				}
				if int(start) > int(end) {
					return nil, NewError(ast.Offset, ast.Length, "slice start cannot be greater than end")
				}
				return left[int(start) : int(end)+1], nil
			}
			left := toString(resultLeft)
			if start < 0 {
				start += float64(len(left))
			}
			if end < 0 {
				end += float64(len(left))
			}
			if err := checkBounds(ast, left, int(start)); err != nil {
				return nil, err
			}
			if int(start) > int(end) {
				return nil, NewError(ast.Offset, ast.Length, "string slice start cannot be greater than end")
			}
			if err := checkBounds(ast, left, int(end)); err != nil {
				return nil, err
			}
			return left[int(start) : int(end)+1], nil
		}
		if isNumber(resultRight) {
			idx, err := toNumber(ast, resultRight)
			if err != nil {
				return nil, err
			}
			if left, ok := resultLeft.([]any); ok {
				if idx < 0 {
					idx += float64(len(left))
				}
				if err := checkBounds(ast, left, int(idx)); err != nil {
					return nil, err
				}
				return left[int(idx)], nil
			}
			left := toString(resultLeft)
			if idx < 0 {
				idx += float64(len(left))
			}
			if err := checkBounds(ast, left, int(idx)); err != nil {
				return nil, err
			}
			return string(left[int(idx)]), nil
		}
		return nil, NewError(ast.Offset, ast.Length, "array index must be number or slice %v", resultRight)
	case NodeSlice:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		ast.Value.([]any)[0] = resultLeft
		ast.Value.([]any)[1] = resultRight
		return ast.Value, nil
	case NodeLiteral:
		return ast.Value, nil
	case NodeSign:
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		right, err := toNumber(ast, resultRight)
		if err != nil {
			return nil, err
		}
		if ast.Value.(string) == "-" {
			right = -right
		}
		return right, nil
	case NodeAdd, NodeSubtract, NodeMultiply, NodeDivide, NodeModulus, NodePower:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		if ast.Type == NodeAdd {
			if isString(resultLeft) || isString(resultRight) {
				return toString(resultLeft) + toString(resultRight), nil
			}
			if isSlice(resultLeft) && isSlice(resultRight) {
				tmp := append([]any{}, resultLeft.([]any)...)
				return append(tmp, resultRight.([]any)...), nil
			}
		}
		if isNumber(resultLeft) && isNumber(resultRight) {
			left, err := toNumber(ast.Left, resultLeft)
			if err != nil {
				return nil, err
			}
			right, err := toNumber(ast.Right, resultRight)
			if err != nil {
				return nil, err
			}
			switch ast.Type {
			case NodeAdd:
				return left + right, nil
			case NodeSubtract:
				return left - right, nil
			case NodeMultiply:
				return left * right, nil
			case NodeDivide:
				if right == 0.0 {
					return nil, NewError(ast.Offset, ast.Length, "cannot divide by zero")
				}
				return left / right, nil
			case NodeModulus:
				if int(right) == 0 {
					return nil, NewError(ast.Offset, ast.Length, "cannot divide by zero")
				}
				return int(left) % int(right), nil
			case NodePower:
				return math.Pow(left, right), nil
			}
		}
		return nil, NewError(ast.Offset, ast.Length, "cannot add incompatible types %v and %v", resultLeft, resultRight)
	case NodeEqual, NodeNotEqual, NodeLessThan, NodeLessThanEqual, NodeGreaterThan, NodeGreaterThanEqual:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		if ast.Type == NodeEqual {
			return deepEqual(resultLeft, resultRight), nil
		}
		if ast.Type == NodeNotEqual {
			return !deepEqual(resultLeft, resultRight), nil
		}

		left, err := toNumber(ast.Left, resultLeft)
		if err != nil {
			return nil, err
		}
		right, err := toNumber(ast.Right, resultRight)
		if err != nil {
			return nil, err
		}

		switch ast.Type {
		case NodeGreaterThan:
			return left > right, nil
		case NodeGreaterThanEqual:
			return left >= right, nil
		case NodeLessThan:
			return left < right, nil
		case NodeLessThanEqual:
			return left <= right, nil
		}
	case NodeAnd, NodeOr:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		left := toBool(resultLeft)
		right := toBool(resultRight)
		switch ast.Type {
		case NodeAnd:
			return left && right, nil
		case NodeOr:
			return left || right, nil
		}
	case NodeBefore, NodeAfter:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		leftTime := toTime(resultLeft)
		if leftTime.IsZero() {
			return nil, NewError(ast.Offset, ast.Length, "unable to convert %v to date or time", resultLeft)
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		rightTime := toTime(resultRight)
		if rightTime.IsZero() {
			return nil, NewError(ast.Offset, ast.Length, "unable to convert %v to date or time", resultRight)
		}
		if ast.Type == NodeBefore {
			return leftTime.Before(rightTime), nil
		} else {
			return leftTime.After(rightTime), nil
		}
	case NodeIn, NodeContains, NodeStartsWith, NodeEndsWith:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		switch ast.Type {
		case NodeIn:
			if a, ok := resultRight.([]any); ok {
				for _, item := range a {
					if deepEqual(item, resultLeft) {
						return true, nil
					}
				}
				return false, nil
			}
			if m, ok := resultRight.(map[string]any); ok {
				if m[toString(resultLeft)] != nil {
					return true, nil
				}
				return false, nil
			}
			if m, ok := resultRight.(map[any]any); ok {
				if m[resultLeft] != nil {
					return true, nil
				}
				return false, nil
			}
			return strings.Contains(toString(resultRight), toString(resultLeft)), nil
		case NodeContains:
			if a, ok := resultLeft.([]any); ok {
				for _, item := range a {
					if deepEqual(item, resultRight) {
						return true, nil
					}
				}
				return false, nil
			}
			if m, ok := resultLeft.(map[string]any); ok {
				if m[toString(resultRight)] != nil {
					return true, nil
				}
				return false, nil
			}
			if m, ok := resultLeft.(map[any]any); ok {
				if m[resultRight] != nil {
					return true, nil
				}
				return false, nil
			}
			return strings.Contains(toString(resultLeft), toString(resultRight)), nil
		case NodeStartsWith:
			return strings.HasPrefix(toString(resultLeft), toString(resultRight)), nil
		case NodeEndsWith:
			return strings.HasSuffix(toString(resultLeft), toString(resultRight)), nil
		}
	case NodeNot:
		resultRight, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		right := toBool(resultRight)
		return !right, nil
	case NodeWhere:
		resultLeft, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		results := []any{}
		if resultLeft == nil {
			return nil, nil
		}
		if m, ok := resultLeft.(map[string]any); ok {
			resultLeft = maps.Values(m)
		}
		if m, ok := resultLeft.(map[any]any); ok {
			values := make([]any, 0, len(m))
			for _, v := range m {
				values = append(values, v)
			}
			resultLeft = values
		}
		for _, item := range resultLeft.([]any) {
			// In an unquoted string scenario it makes no sense for the first/only
			// token after a `where` clause to be treated as a string. Instead we
			// treat a `where` the same as a field select `.` in this scenario.
			i.prevFieldSelect = true
			resultRight, _ := i.run(ast.Right, item)
			if i.strict && err != nil {
				return nil, err
			}
			if toBool(resultRight) {
				results = append(results, item)
			}
		}
		return results, nil
	}
	return nil, nil
}
