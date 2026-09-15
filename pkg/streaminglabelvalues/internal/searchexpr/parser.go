// SPDX-License-Identifier: AGPL-3.0-only

// Package searchexpr parses the boolean expression language used to narrow
// streaming label-name and label-value search candidates.
package searchexpr

import (
	"fmt"
	"strings"
)

// Expr is a search expression AST node.
//
// The interface is sealed so that every expression used by a future compiler
// has the semantics defined by this package.
type Expr interface {
	expr()
}

// Term matches one search term.
type Term struct {
	Value string
}

func (Term) expr() {}

// And accepts candidates accepted by both children.
type And struct {
	Left  Expr
	Right Expr
}

func (And) expr() {}

// Or accepts candidates accepted by either child.
type Or struct {
	Left  Expr
	Right Expr
}

func (Or) expr() {}

// Parse parses terms combined with AND, OR, and parentheses. AND binds more
// tightly than OR. Operators are case-insensitive; quote a term to search for
// the literal text "and" or "or".
func Parse(input string) (Expr, error) {
	tokens, err := tokenize(input)
	if err != nil {
		return nil, err
	}

	p := parser{tokens: tokens}
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokenEOF {
		return nil, p.errorf("unexpected %s", p.peek().describe())
	}
	return expr, nil
}

type tokenKind uint8

const (
	tokenEOF tokenKind = iota
	tokenTerm
	tokenAnd
	tokenOr
	tokenLeftParen
	tokenRightParen
)

type token struct {
	kind  tokenKind
	value string
	pos   int
}

func (t token) describe() string {
	switch t.kind {
	case tokenEOF:
		return "end of expression"
	case tokenTerm:
		return fmt.Sprintf("term %q", t.value)
	case tokenAnd:
		return "AND"
	case tokenOr:
		return "OR"
	case tokenLeftParen:
		return "("
	case tokenRightParen:
		return ")"
	default:
		return "token"
	}
}

func tokenize(input string) ([]token, error) {
	tokens := make([]token, 0, strings.Count(input, " ")+1)
	for pos := 0; pos < len(input); {
		if input[pos] == ' ' || input[pos] == '\t' || input[pos] == '\n' || input[pos] == '\r' {
			pos++
			continue
		}

		switch input[pos] {
		case '(':
			tokens = append(tokens, token{kind: tokenLeftParen, pos: pos})
			pos++
		case ')':
			tokens = append(tokens, token{kind: tokenRightParen, pos: pos})
			pos++
		case '"':
			value, next, err := quotedTerm(input, pos)
			if err != nil {
				return nil, err
			}
			if value == "" {
				return nil, fmt.Errorf("search expression: empty quoted term at position %d", pos)
			}
			tokens = append(tokens, token{kind: tokenTerm, value: value, pos: pos})
			pos = next
		default:
			start := pos
			for pos < len(input) && input[pos] != ' ' && input[pos] != '\t' && input[pos] != '\n' && input[pos] != '\r' && input[pos] != '(' && input[pos] != ')' {
				pos++
			}
			value := input[start:pos]
			tokens = append(tokens, keywordToken(value, start))
		}
	}

	return append(tokens, token{kind: tokenEOF, pos: len(input)}), nil
}

func quotedTerm(input string, start int) (string, int, error) {
	var value strings.Builder
	for pos := start + 1; pos < len(input); pos++ {
		switch input[pos] {
		case '"':
			return value.String(), pos + 1, nil
		case '\\':
			if pos+1 == len(input) {
				break
			}
			pos++
			value.WriteByte(input[pos])
		default:
			value.WriteByte(input[pos])
		}
	}
	return "", 0, fmt.Errorf("search expression: unterminated quoted term at position %d", start)
}

func keywordToken(value string, pos int) token {
	switch strings.ToUpper(value) {
	case "AND":
		return token{kind: tokenAnd, pos: pos}
	case "OR":
		return token{kind: tokenOr, pos: pos}
	default:
		return token{kind: tokenTerm, value: value, pos: pos}
	}
}

type parser struct {
	tokens []token
	index  int
}

func (p *parser) parseOr() (Expr, error) {
	return p.parseBinary(tokenOr, p.parseAnd, func(left, right Expr) Expr {
		return Or{Left: left, Right: right}
	})
}

func (p *parser) parseAnd() (Expr, error) {
	return p.parseBinary(tokenAnd, p.parsePrimary, func(left, right Expr) Expr {
		return And{Left: left, Right: right}
	})
}

func (p *parser) parseBinary(operator tokenKind, parseOperand func() (Expr, error), combine func(Expr, Expr) Expr) (Expr, error) {
	left, err := parseOperand()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == operator {
		p.next()
		right, err := parseOperand()
		if err != nil {
			return nil, err
		}
		left = combine(left, right)
	}
	return left, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	token := p.next()
	switch token.kind {
	case tokenTerm:
		return Term{Value: token.value}, nil
	case tokenLeftParen:
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tokenRightParen {
			return nil, p.errorf("expected ), got %s", p.peek().describe())
		}
		p.next()
		return expr, nil
	default:
		return nil, p.errorf("expected term or (, got %s", token.describe())
	}
}

func (p *parser) peek() token {
	return p.tokens[p.index]
}

func (p *parser) next() token {
	token := p.peek()
	if token.kind != tokenEOF {
		p.index++
	}
	return token
}

func (p *parser) errorf(format string, args ...any) error {
	return fmt.Errorf("search expression: "+format+" at position %d", append(args, p.peek().pos)...)
}
