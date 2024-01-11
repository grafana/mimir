package mexpr

import (
	"math"
	"strconv"
)

// NodeType defines the type of the abstract syntax tree node.
type NodeType uint8

// Possible node types
const (
	NodeUnknown NodeType = iota
	NodeIdentifier
	NodeLiteral
	NodeAdd
	NodeSubtract
	NodeMultiply
	NodeDivide
	NodeModulus
	NodePower
	NodeEqual
	NodeNotEqual
	NodeLessThan
	NodeLessThanEqual
	NodeGreaterThan
	NodeGreaterThanEqual
	NodeAnd
	NodeOr
	NodeNot
	NodeFieldSelect
	NodeArrayIndex
	NodeSlice
	NodeSign
	NodeIn
	NodeContains
	NodeStartsWith
	NodeEndsWith
	NodeBefore
	NodeAfter
	NodeWhere
)

// Node is a unit of the binary tree that makes up the abstract syntax tree.
type Node struct {
	Type   NodeType
	Length uint8
	Offset uint16
	Left   *Node
	Right  *Node
	Value  interface{}
}

// String converts the node to a string representation (basically the node name
// or the node's value for identifiers/literals).
func (n Node) String() string {
	switch n.Type {
	case NodeIdentifier, NodeLiteral:
		return toString(n.Value)
	case NodeAdd:
		return "+"
	case NodeSubtract:
		return "-"
	case NodeMultiply:
		return "*"
	case NodeDivide:
		return "/"
	case NodeModulus:
		return "%"
	case NodePower:
		return "^"
	case NodeEqual:
		return "=="
	case NodeNotEqual:
		return "!="
	case NodeLessThan:
		return "<"
	case NodeLessThanEqual:
		return "<="
	case NodeGreaterThan:
		return ">"
	case NodeGreaterThanEqual:
		return ">="
	case NodeAnd:
		return "and"
	case NodeOr:
		return "or"
	case NodeNot:
		return "not"
	case NodeFieldSelect:
		return "."
	case NodeArrayIndex:
		return "[]"
	case NodeSlice:
		return ":"
	case NodeIn:
		return "in"
	case NodeContains:
		return "contains"
	case NodeStartsWith:
		return "startsWith"
	case NodeEndsWith:
		return "endsWith"
	case NodeBefore:
		return "before"
	case NodeAfter:
		return "after"
	case NodeWhere:
		return "where"
	}

	return ""
}

// Dot returns a graphviz-compatible dot output, which can be used to render
// the parse tree at e.g. https://dreampuf.github.io/GraphvizOnline/ or
// locally. You must wrap the output with `graph G {` and `}`.
func (n Node) Dot(prefix string) string {
	value := "\"" + prefix + n.String() + "\" [label=\"" + n.String() + "\"];\n"
	if n.Left != nil {
		value += "\"" + prefix + n.String() + "\" -- \"" + prefix + "l" + n.Left.String() + "\"\n"
		value += n.Left.Dot(prefix+"l") + "\n"
	}
	if n.Right != nil {
		value += "\"" + prefix + n.String() + "\" -- \"" + prefix + "r" + n.Right.String() + "\"\n"
		value += n.Right.Dot(prefix+"r") + "\n"
	}
	return value
}

// bindingPowers for different tokens. Not listed means zero. The higher the
// number, the higher the token is in the order of operations.
var bindingPowers = map[TokenType]int{
	TokenOr:            1,
	TokenAnd:           2,
	TokenWhere:         3,
	TokenStringCompare: 4,
	TokenComparison:    5,
	TokenSlice:         5,
	TokenAddSub:        10,
	TokenMulDiv:        15,
	TokenNot:           40,
	TokenDot:           45,
	TokenPower:         50,
	TokenLeftBracket:   60,
	TokenLeftParen:     70,
}

// precomputeLiterals takes two `NodeLiteral` nodes and a math operation and
// generates a single literal node for the resutl. This prevents the interpreter
// from needing to re-compute the value each time.
func precomputeLiterals(offset uint16, nodeType NodeType, left, right *Node) (*Node, Error) {
	leftValue, err := toNumber(left, left.Value)
	if err != nil {
		return nil, err
	}
	rightValue, err := toNumber(right, right.Value)
	if err != nil {
		return nil, err
	}
	l := left.Length + right.Length
	switch nodeType {
	case NodeAdd:
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: leftValue + rightValue}, nil
	case NodeSubtract:
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: leftValue - rightValue}, nil
	case NodeMultiply:
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: leftValue * rightValue}, nil
	case NodeDivide:
		if rightValue == 0 {
			return nil, NewError(offset, 1, "cannot divide by zero")
		}
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: leftValue / rightValue}, nil
	case NodeModulus:
		if int(rightValue) == 0 {
			return nil, NewError(offset, 1, "cannot divide by zero")
		}
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: float64(int(leftValue) % int(rightValue))}, nil
	case NodePower:
		return &Node{Type: NodeLiteral, Offset: offset, Length: l, Value: math.Pow(leftValue, rightValue)}, nil
	}
	return nil, NewError(offset, 1, "cannot precompute unknown operator")
}

// Parser takes a lexer and parses its tokens into an abstract syntax tree.
type Parser interface {
	// Parse the expression and return the root node.
	Parse() (*Node, Error)
}

// NewParser creates a new parser that uses the given lexer to get and process
// tokens into an abstract syntax tree.
func NewParser(lexer Lexer) Parser {
	return &parser{
		lexer: lexer,
	}
}

// parser is an implementation of a Pratt or top-down operator precedence parser
type parser struct {
	lexer Lexer
	token *Token
}

func (p *parser) advance() Error {
	t, err := p.lexer.Next()
	if err != nil {
		return err
	}
	p.token = t
	return nil
}

func (p *parser) parse(bindingPower int) (*Node, Error) {
	leftToken := *p.token
	if err := p.advance(); err != nil {
		return nil, err
	}
	leftNode, err := p.nud(&leftToken)
	if err != nil {
		return nil, err
	}
	currentToken := *p.token
	for bindingPower < bindingPowers[currentToken.Type] {
		if leftNode == nil {
			return nil, nil
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		leftNode, err = p.led(&currentToken, leftNode)
		if err != nil {
			return nil, err
		}
		currentToken = *p.token
	}
	return leftNode, nil
}

// ensure the current token is `typ`, returning the `result` unless `err` is
// set or some other error occurs. Advances past the expected token type.
func (p *parser) ensure(result *Node, err Error, typ TokenType) (*Node, Error) {
	if err != nil {
		return nil, err
	}
	if p.token.Type == typ {
		if err := p.advance(); err != nil {
			return nil, err
		}
		return result, nil
	}

	extra := ""
	if typ == TokenEOF && p.token.Type == TokenIdentifier {
		switch p.token.Value {
		case "startswith", "beginswith", "beginsWith", "hasprefix", "hasPrefix":
			extra = " (did you mean `startsWith`?)"
		case "endswith", "hassuffix", "hasSuffix":
			extra = " (did you mean `endsWith`?)"
		case "contains":
			extra = " (did you mean `in`?)"
		}
	}

	return nil, NewError(p.token.Offset, p.token.Length, "expected %s but found %s%s", typ, p.token.Type, extra)
}

// nud: null denotation. These nodes have no left context and only
// consume to the right. Examples: identifiers, numbers, unary operators like
// minus.
func (p *parser) nud(t *Token) (*Node, Error) {
	switch t.Type {
	case TokenIdentifier:
		return &Node{Type: NodeIdentifier, Value: t.Value, Offset: t.Offset, Length: t.Length}, nil
	case TokenNumber:
		f, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, NewError(p.token.Offset, p.token.Length, err.Error())
		}
		return &Node{Type: NodeLiteral, Value: f, Offset: t.Offset, Length: t.Length}, nil
	case TokenString:
		return &Node{Type: NodeLiteral, Value: t.Value, Offset: t.Offset, Length: t.Length}, nil
	case TokenLeftParen:
		result, err := p.parse(0)
		return p.ensure(result, err, TokenRightParen)
	case TokenNot:
		offset := t.Offset
		result, err := p.parse(bindingPowers[t.Type])
		if err != nil {
			return nil, err
		}
		return &Node{Type: NodeNot, Offset: offset, Length: uint8(t.Offset + uint16(t.Length) - offset), Right: result}, nil
	case TokenAddSub:
		value := t.Value
		offset := t.Offset
		result, err := p.parse(bindingPowers[t.Type])
		if err != nil {
			return nil, err
		}
		return &Node{Type: NodeSign, Value: value, Offset: offset, Length: uint8(t.Offset + uint16(t.Length) - offset), Right: result}, nil
	case TokenSlice:
		offset := t.Offset
		result, err := p.parse(bindingPowers[t.Type])
		if err != nil {
			return nil, err
		}
		// Create a dummy left node with value 0, the start of the slice. This also
		// sets the parent node's value to a pre-allocated list of [0, 0] which is
		// used later by the interpreter. It prevents additional allocations.
		return &Node{Type: NodeSlice, Offset: offset, Length: uint8(t.Offset + uint16(t.Length) - offset), Left: &Node{Type: NodeLiteral, Value: 0.0, Offset: offset}, Right: result, Value: []interface{}{0.0, 0.0}}, nil
	case TokenRightParen:
		return nil, NewError(t.Offset, t.Length, "unexpected right-paren")
	case TokenRightBracket:
		return nil, NewError(t.Offset, t.Length, "unexpected right-bracket")
	case TokenEOF:
		return nil, NewError(t.Offset, t.Length, "incomplete expression, EOF found")
	}
	return nil, nil
}

// newNodeParseRight creates a new node with the right tree set to the
// output of recursively parsing until a lower binding power is encountered.
func (p *parser) newNodeParseRight(left *Node, t *Token, typ NodeType, bindingPower int) (*Node, Error) {
	offset := t.Offset
	right, err := p.parse(bindingPower)
	if err != nil {
		return nil, err
	}
	if right == nil {
		return nil, NewError(t.Offset, t.Length, "missing right operand")
	}
	return &Node{Type: typ, Offset: offset, Length: uint8(p.token.Offset + uint16(p.token.Length) - offset), Left: left, Right: right}, nil
}

// led: left denotation. These tokens produce nodes that operate on two operands
// a left and a right. Examples: addition, multiplication, etc.
func (p *parser) led(t *Token, n *Node) (*Node, Error) {
	switch t.Type {
	case TokenAddSub, TokenMulDiv, TokenPower:
		var nodeType NodeType
		switch t.Value[0] {
		case '+':
			nodeType = NodeAdd
		case '-':
			nodeType = NodeSubtract
		case '*':
			nodeType = NodeMultiply
		case '/':
			nodeType = NodeDivide
		case '%':
			nodeType = NodeModulus
		case '^':
			nodeType = NodePower
		}
		offset := t.Offset
		binding := bindingPowers[t.Type]
		if t.Type == TokenPower {
			// Power operations should be right-associative, so we lower the binding
			// power slightly so it prefers going right.
			binding--
		}
		right, err := p.parse(binding)
		if err != nil {
			return nil, err
		}
		if right == nil {
			return nil, NewError(t.Offset, t.Length, "missing right operand")
		}
		if n.Type == NodeLiteral && right.Type == NodeLiteral {
			if !(isString(n.Value) || isString(right.Value)) {
				return precomputeLiterals(offset, nodeType, n, right)
			}
		}
		return &Node{Type: nodeType, Offset: offset, Length: uint8(t.Offset + uint16(t.Length) - offset), Left: n, Right: right, Value: 0.0}, nil
	case TokenComparison:
		var nodeType NodeType
		switch t.Value {
		case "==":
			nodeType = NodeEqual
		case "!=":
			nodeType = NodeNotEqual
		case "<":
			nodeType = NodeLessThan
		case "<=":
			nodeType = NodeLessThanEqual
		case ">":
			nodeType = NodeGreaterThan
		case ">=":
			nodeType = NodeGreaterThanEqual
		}
		return p.newNodeParseRight(n, t, nodeType, bindingPowers[t.Type])
	case TokenAnd:
		return p.newNodeParseRight(n, t, NodeAnd, bindingPowers[t.Type])
	case TokenOr:
		return p.newNodeParseRight(n, t, NodeOr, bindingPowers[t.Type])
	case TokenStringCompare:
		var nodeType NodeType
		switch t.Value {
		case "in":
			nodeType = NodeIn
		case "contains":
			nodeType = NodeContains
		case "startsWith":
			nodeType = NodeStartsWith
		case "endsWith":
			nodeType = NodeEndsWith
		case "before":
			nodeType = NodeBefore
		case "after":
			nodeType = NodeAfter
		}
		return p.newNodeParseRight(n, t, nodeType, bindingPowers[t.Type])
	case TokenWhere:
		return p.newNodeParseRight(n, t, NodeWhere, bindingPowers[t.Type])
	case TokenDot:
		return p.newNodeParseRight(n, t, NodeFieldSelect, bindingPowers[t.Type])
	case TokenLeftBracket:
		n, err := p.newNodeParseRight(n, t, NodeArrayIndex, 0)
		return p.ensure(n, err, TokenRightBracket)
	case TokenSlice:
		if p.token.Type == TokenRightBracket {
			// This sets the parent node's value to a pre-allocated list of [0, 0]
			// which is used later by the interpreter. It prevents additional
			// allocations.
			return &Node{Type: NodeSlice, Offset: t.Offset, Length: t.Length, Left: n, Right: &Node{Type: NodeLiteral, Offset: t.Offset, Value: -1.0}, Value: []interface{}{0.0, 0.0}}, nil
		}
		nn, err := p.newNodeParseRight(n, t, NodeSlice, bindingPowers[t.Type])
		if err != nil {
			return nil, err
		}
		nn.Value = []interface{}{0.0, 0.0}
		return nn, nil
	}
	return nil, NewError(t.Offset, t.Length, "unexpected token %s", t.Type)
}

func (p *parser) Parse() (*Node, Error) {
	if err := p.advance(); err != nil {
		return nil, err
	}
	n, err := p.parse(0)
	return p.ensure(n, err, TokenEOF)
}
