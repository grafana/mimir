// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

type LimitedAllocator struct {
	Allocator memory.Allocator
	inuse     uint64
	limit     uint64
}

func NewLimitedAllocator(allocator memory.Allocator, limit uint64) *LimitedAllocator {
	return &LimitedAllocator{
		Allocator: allocator,
		limit:     limit,
	}
}

var _ memory.Allocator = &LimitedAllocator{}

type LimitError struct {
	Request uint64
	Inuse   uint64
	Limit   uint64
}

var _ error = LimitError{}

// NewLimitErrorFromError extracts a formatted limit error.
func NewLimitErrorFromError(err error) (error, bool) {
	var lerr LimitError
	if errors.As(err, &lerr) {
		return lerr, true
	}
	return err, false
}

func (le LimitError) Error() string {
	return fmt.Sprintf("memory limit exceeded: requested %d out of %d (in-use=%d)", le.Request, le.Limit, le.Inuse)
}

func (_ LimitError) Is(tgt error) bool {
	_, ok := tgt.(LimitError)
	return ok
}

func (l *LimitedAllocator) Inuse() uint64 {
	return l.inuse
}

func (l *LimitedAllocator) Allocate(size int) []byte {
	change := uint64(size)
	if l.inuse+change > l.limit {
		err := LimitError{
			Request: change,
			Inuse:   l.inuse,
			Limit:   l.limit,
		}
		panic(err)
	}

	res := l.Allocator.Allocate(size)

	// This update will be skipped if Allocate() panics.
	l.inuse += change
	return res
}

func (l *LimitedAllocator) Reallocate(size int, b []byte) []byte {
	change := uint64(size - len(b))
	if l.inuse+change > l.limit {
		err := LimitError{
			Request: change,
			Inuse:   l.inuse,
			Limit:   l.limit,
		}
		panic(err)
	}

	res := l.Allocator.Reallocate(size, b)

	// This update will be skipped if Reallocate() panics.
	l.inuse += change
	return res
}

func (l *LimitedAllocator) Free(b []byte) {
	l.Allocator.Free(b)

	// This update will be skipped if Free() panics.
	l.inuse -= uint64(len(b))
}
