// SPDX-License-Identifier: AGPL-3.0-only

package types

import "github.com/pkg/errors"

type contextKey int

const (
	ContextEnableDelayedNameRemoval contextKey = iota
	ContextDeduplicateAndMergeStackCount
)

var (
	ErrEnableDelayedNameRemovalNotFound      = errors.New("enableDelayedNameRemoval not found in context")
	ErrDeduplicateAndMergeStackCountNotFound = errors.New("deduplicateAndMergeStackCount not found in context")
)
