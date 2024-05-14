// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"errors"
	"fmt"
)

type NotSupportedError struct {
	reason string
}

func NewNotSupportedError(reason string) error {
	return NotSupportedError{reason}
}

func (e NotSupportedError) Error() string {
	return fmt.Sprintf("not supported by streaming engine: %v", e.reason)
}

func (e NotSupportedError) Is(target error) bool {
	return errors.As(target, &NotSupportedError{})
}
