package shorthand

import (
	"fmt"
	"strconv"
)

// Error represents an error at a specific location.
type Error interface {
	Error() string

	// Offset returns the character offset of the error within the experssion.
	Offset() uint

	// Length returns the length in bytes after the offset where the error ends.
	Length() uint

	// Pretty prints out a message with a pointer to the source location of the
	// error.
	Pretty() string
}

type exprErr struct {
	source  *string
	offset  uint
	length  uint
	message string
}

func (e *exprErr) Error() string {
	return e.message
}

func (e *exprErr) Offset() uint {
	return e.offset
}

func (e *exprErr) Length() uint {
	return e.length
}

func (e *exprErr) Pretty() string {
	// Figure out which absolute line we are on.
	lineNo := 1
	for i := 0; i < int(e.offset); i++ {
		if (*e.source)[i] == '\n' {
			lineNo++
		}
	}

	// Determine lines of context to show if multi-line.
	start := int(e.offset)
	if start > len(*e.source)-1 {
		start = len(*e.source) - 1
	}

	lineStart := 0
	lines := 0
	for start > 0 {
		if (*e.source)[start] == '\n' {
			if lines == 0 {
				lineStart = start + 1
			}
			lines++
			if lines >= 4 {
				start++
				break
			}
		}
		start--
	}

	end := int(e.offset)
	for end < len(*e.source) && (*e.source)[end] != '\n' {
		end++
	}

	// Generate a nice error message with context.
	msg := e.Error() + " at line " + strconv.Itoa(lineNo) + " col " + strconv.Itoa(int(e.offset-uint(lineStart))+1) + "\n" + (*e.source)[start:end] + "\n"
	for i := uint(lineStart); i < e.offset; i++ {
		msg += "."
	}
	for i := 0; i < end-int(e.offset); i++ {
		msg += "^"
	}
	return msg
}

// NewError creates a new error at a specific location.
func NewError(source *string, offset uint, length uint, format string, a ...interface{}) Error {
	if length < 1 {
		length = 1
	}
	return &exprErr{
		source:  source,
		offset:  offset,
		length:  length,
		message: fmt.Sprintf(format, a...),
	}
}
