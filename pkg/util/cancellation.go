// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"fmt"
)

type cancellationError struct {
	inner error
}

func NewCancellationError(err error) error {
	return cancellationError{err}
}

func NewCancellationErrorf(format string, args ...any) error {
	return NewCancellationError(fmt.Errorf(format, args...))
}

func (e cancellationError) Error() string {
	return "context canceled: " + e.inner.Error()
}

func (e cancellationError) Is(err error) bool {
	return err == context.Canceled
}

func (e cancellationError) Unwrap() error {
	return e.inner
}
