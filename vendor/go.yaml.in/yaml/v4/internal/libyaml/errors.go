package libyaml

import (
	"fmt"
	"strings"
)

type MarkedYAMLError struct {
	// optional context
	ContextMark    Mark
	ContextMessage string

	Mark    Mark
	Message string
}

func (e MarkedYAMLError) Error() string {
	var builder strings.Builder
	builder.WriteString("yaml: ")
	if len(e.ContextMessage) > 0 {
		fmt.Fprintf(&builder, "%s at %s: ", e.ContextMessage, e.ContextMark)
	}
	if len(e.ContextMessage) == 0 || e.ContextMark != e.Mark {
		fmt.Fprintf(&builder, "%s: ", e.Mark)
	}
	builder.WriteString(e.Message)
	return builder.String()
}

type ParserError MarkedYAMLError

func (e ParserError) Error() string {
	return MarkedYAMLError(e).Error()
}

type ScannerError MarkedYAMLError

func (e ScannerError) Error() string {
	return MarkedYAMLError(e).Error()
}

type ReaderError struct {
	Offset int
	Value  int
	Err    error
}

func (e ReaderError) Error() string {
	return fmt.Sprintf("yaml: offset %d: %s", e.Offset, e.Err)
}

func (e ReaderError) Unwrap() error {
	return e.Err
}

type EmitterError struct {
	Message string
}

func (e EmitterError) Error() string {
	return fmt.Sprintf("yaml: %s", e.Message)
}

type WriterError struct {
	Err error
}

func (e WriterError) Error() string {
	return fmt.Sprintf("yaml: %s", e.Err)
}

func (e WriterError) Unwrap() error {
	return e.Err
}
