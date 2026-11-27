// SPDX-License-Identifier: AGPL-3.0-only
//go:build !unix

package mmap

import (
	"errors"
	"os"
)

const Supported = false

func Map(_ *os.File, _ int, _ bool) ([]byte, error) { return nil, errors.ErrUnsupported }
func Anonymous(_ int) ([]byte, error)               { return nil, errors.ErrUnsupported }
func Flush(_ []byte) error                          { return errors.ErrUnsupported }
func Unmap(_ []byte) error                          { return errors.ErrUnsupported }
