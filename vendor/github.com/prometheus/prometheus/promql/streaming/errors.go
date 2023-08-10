package streaming

import (
	"errors"
	"fmt"
)

var ErrNotSupported = errors.New("not supported by streaming engine")

func NewNotSupportedError(detail string) error {
	return fmt.Errorf("%w: %s", ErrNotSupported, detail)
}
