package mexpr

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
)

type valueType string

const (
	typeUnknown valueType = "unknown"
	typeBool    valueType = "boolean"
	typeNumber  valueType = "number"
	typeString  valueType = "string"
	typeArray   valueType = "array"
	typeObject  valueType = "object"
)

type schema struct {
	typeName   valueType
	items      *schema
	properties map[string]*schema
}

func (s *schema) String() string {
	if s.isArray() {
		return fmt.Sprintf("%s[%s]", s.typeName, s.items)
	}
	if s.isObject() {
		return fmt.Sprintf("%s{%v}", s.typeName, maps.Keys(s.properties))
	}
	return string(s.typeName)
}

func (s *schema) isNumber() bool {
	return s != nil && s.typeName == typeNumber
}

func (s *schema) isString() bool {
	return s != nil && s.typeName == typeString
}

func (s *schema) isArray() bool {
	return s != nil && s.typeName == typeArray
}

func (s *schema) isObject() bool {
	return s != nil && s.typeName == typeObject
}

var (
	schemaBool   = newSchema(typeBool)
	schemaNumber = newSchema(typeNumber)
	schemaString = newSchema(typeString)
)

func newSchema(t valueType) *schema {
	return &schema{typeName: t}
}

func getSchema(v any) *schema {
	switch i := v.(type) {
	case bool:
		return schemaBool
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return schemaNumber
	case string, []byte:
		return schemaString
	case []any:
		s := newSchema(typeArray)
		if len(i) > 0 {
			s.items = getSchema(i[0])
		}
		return s
	case map[string]any:
		m := newSchema(typeObject)
		m.properties = make(map[string]*schema, len(i))
		for k, v := range i {
			m.properties[k] = getSchema(v)
		}
		return m
	case map[any]any:
		m := newSchema(typeObject)
		m.properties = make(map[string]*schema, len(i))
		for k, v := range i {
			m.properties[toString(k)] = getSchema(v)
		}
		return m
	}
	return newSchema(typeUnknown)
}

// TypeChecker checks to ensure types used for operations will work.
type TypeChecker interface {
	Run(value any) Error
}

// NewTypeChecker returns a type checker for the given AST.
func NewTypeChecker(ast *Node, options ...InterpreterOption) TypeChecker {
	unquoted := false

	for _, opt := range options {
		switch opt {
		case UnquotedStrings:
			unquoted = true
		}
	}

	return &typeChecker{
		ast:      ast,
		unquoted: unquoted,
	}
}

type typeChecker struct {
	ast             *Node
	prevFieldSelect bool
	unquoted        bool
}

func (i *typeChecker) Run(value any) Error {
	_, err := i.run(i.ast, value)
	return err
}

func (i *typeChecker) runBoth(ast *Node, value any) (*schema, *schema, Error) {
	leftType, err := i.run(ast.Left, value)
	if err != nil {
		return nil, nil, err
	}
	rightType, err := i.run(ast.Right, value)
	if err != nil {
		return nil, nil, err
	}
	return leftType, rightType, nil
}

func (i *typeChecker) run(ast *Node, value any) (*schema, Error) {
	fromSelect := i.prevFieldSelect
	i.prevFieldSelect = false

	switch ast.Type {
	case NodeIdentifier:
		switch ast.Value.(string) {
		case "@":
			if s, ok := value.(*schema); ok {
				return s, nil
			}
			return getSchema(value), nil
		case "length":
			return schemaNumber, nil
		case "lower", "upper":
			return schemaString, nil
		}
		errValue := value
		if s, ok := value.(*schema); ok {
			if v, ok := s.properties[ast.Value.(string)]; ok {
				return v, nil
			}
			keys := []string{}
			for k := range s.properties {
				keys = append(keys, k)
			}
			errValue = "map with keys [" + strings.Join(keys, ", ") + "]"
		}
		if m, ok := value.(map[string]any); ok {
			if v, ok := m[ast.Value.(string)]; ok {
				return getSchema(v), nil
			}
			keys := []string{}
			for k := range m {
				keys = append(keys, k)
			}
			errValue = "map with keys [" + strings.Join(keys, ", ") + "]"
		}
		if m, ok := value.(map[any]any); ok {
			if v, ok := m[ast.Value]; ok {
				return getSchema(v), nil
			}
			keys := []string{}
			for k := range m {
				keys = append(keys, toString(k))
			}
			errValue = "map with keys [" + strings.Join(keys, ", ") + "]"
		}
		if i.unquoted && !fromSelect {
			// Identifiers not found in the map are treated as strings, but only if
			// the previous item was not a `.` like `obj.field`.
			return schemaString, nil
		}
		return nil, NewError(ast.Offset, ast.Length, "no property %v in %v", ast.Value, errValue)
	case NodeFieldSelect:
		i.prevFieldSelect = true
		leftType, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		i.prevFieldSelect = true
		return i.run(ast.Right, leftType)
	case NodeArrayIndex:
		leftType, rightType, err := i.runBoth(ast, value)
		if err != nil {
			return nil, err
		}
		if !(leftType.isString() || leftType.isArray()) {
			return nil, NewError(ast.Offset, ast.Length, "can only index strings or arrays but got %v", leftType)
		}
		if rightType.isArray() {
			// This is a slice!
			return leftType, nil
		}
		if rightType.isNumber() {
			if leftType.isString() {
				return leftType, nil
			}
			return leftType.items, nil
		}
		return nil, NewError(ast.Offset, ast.Length, "array index must be number or slice but found %v", rightType)
	case NodeSlice:
		leftType, rightType, err := i.runBoth(ast, value)
		if err != nil {
			return nil, err
		}
		if !leftType.isNumber() {
			return nil, NewError(ast.Offset, ast.Length, "slice index must be a number but found %s", leftType)
		}
		if !rightType.isNumber() {
			return nil, NewError(ast.Offset, ast.Length, "slice index must be a number but found %s", rightType)
		}
		s := newSchema(typeArray)
		s.items = leftType
		return s, nil
	case NodeLiteral:
		return getSchema(ast.Value), nil
	case NodeSign:
		rightType, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		if !rightType.isNumber() {
			return nil, NewError(ast.Offset, ast.Length, "expected number but found %s", rightType)
		}
		return schemaNumber, nil
	case NodeAdd, NodeSubtract, NodeMultiply, NodeDivide, NodeModulus, NodePower:
		leftType, rightType, err := i.runBoth(ast, value)
		if err != nil {
			return nil, err
		}
		if ast.Type == NodeAdd {
			if leftType.isString() || rightType.isString() {
				return schemaString, nil
			}
			if leftType.isArray() && rightType.isArray() {
				if leftType.items.typeName != rightType.items.typeName {
					return nil, NewError(ast.Offset, ast.Length, "array item types don't match: %s vs %s", leftType.items, rightType.items)
				}
				return leftType, nil
			}
		}
		if leftType.isNumber() && rightType.isNumber() {
			return leftType, nil
		}
		return nil, NewError(ast.Offset, ast.Length, "cannot operate on incompatible types %v and %v", leftType.typeName, rightType.typeName)
	case NodeLessThan, NodeLessThanEqual, NodeGreaterThan, NodeGreaterThanEqual:
		leftType, rightType, err := i.runBoth(ast, value)
		if err != nil {
			return nil, err
		}
		if !leftType.isNumber() || !rightType.isNumber() {
			return nil, NewError(ast.Offset, ast.Length, "cannot compare %s with %s", leftType, rightType)
		}
		return schemaBool, nil
	case NodeEqual, NodeNotEqual, NodeAnd, NodeOr, NodeIn, NodeContains, NodeStartsWith, NodeEndsWith, NodeBefore, NodeAfter:
		_, _, err := i.runBoth(ast, value)
		if err != nil {
			return nil, err
		}
		return schemaBool, nil
	case NodeWhere:
		leftType, err := i.run(ast.Left, value)
		if err != nil {
			return nil, err
		}
		if leftType.isObject() {
			keys := maps.Keys(leftType.properties)
			sort.Strings(keys)
			if len(keys) > 0 {
				// Pick the first prop as the representative item type.
				prop := leftType.properties[keys[0]]
				leftType = newSchema(typeArray)
				leftType.items = prop
			}
		}
		if !leftType.isArray() || leftType.items == nil {
			return nil, NewError(ast.Offset, ast.Length, "where clause requires a non-empty array or object, but found %s", leftType)
		}
		// In an unquoted string scenario it makes no sense for the first/only
		// token after a `where` clause to be treated as a string. Instead we
		// treat a `where` the same as a field select `.` in this scenario.
		i.prevFieldSelect = true
		_, err = i.run(ast.Right, leftType.items)
		if err != nil {
			return nil, err
		}
		return leftType, nil
	case NodeNot:
		_, err := i.run(ast.Right, value)
		if err != nil {
			return nil, err
		}
		return schemaBool, nil
	}
	return nil, NewError(ast.Offset, ast.Length, "unexpected node %v", ast)
}
