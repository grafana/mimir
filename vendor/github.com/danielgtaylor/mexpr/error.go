package mexpr

import "fmt"

// Error represents an error at a specific location.
type Error interface {
	Error() string

	// Offset returns the character offset of the error within the experssion.
	Offset() uint16

	// Length returns the length in bytes after the offset where the error ends.
	Length() uint8

	// Pretty prints out a message with a pointer to the source location of the
	// error.
	Pretty(source string) string
}

type exprErr struct {
	offset  uint16
	length  uint8
	message string
}

func (e *exprErr) Error() string {
	return e.message
}

func (e *exprErr) Offset() uint16 {
	return e.offset
}

func (e *exprErr) Length() uint8 {
	return e.length
}

func (e *exprErr) Pretty(source string) string {
	msg := e.Error() + "\n" + source + "\n"
	for i := uint16(0); i < e.offset; i++ {
		msg += "."
	}
	for i := uint8(0); i < e.length; i++ {
		msg += "^"
	}
	return msg
}

// NewError creates a new error at a specific location.
func NewError(offset uint16, length uint8, format string, a ...interface{}) Error {
	return &exprErr{
		offset:  offset,
		length:  length,
		message: fmt.Sprintf(format, a...),
	}
}
