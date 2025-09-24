// Copyright 2025 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains methods and types in addition to what's already in postings.go to provide compatibility
// with non-prometheus code. These are in a separate file to reduce the diff with Prometheus and make conflicts less likely.

package index

import (
	"context"

	"github.com/prometheus/prometheus/storage"
)

// PostingsCloner takes an existing Postings and allows independently clone them.
type PostingsCloner struct {
	ids []storage.SeriesRef
	err error
}

// NewPostingsCloner takes an existing Postings and allows independently clone them.
// The instance provided shouldn't have been used before (no Next() calls should have been done)
// and it shouldn't be used once provided to the PostingsCloner.
func NewPostingsCloner(p Postings) *PostingsCloner {
	ids, err := ExpandPostings(p)
	return &PostingsCloner{ids: ids, err: err}
}

// Clone returns another independent Postings instance.
func (c *PostingsCloner) Clone(context.Context) Postings {
	if c.err != nil {
		return ErrPostings(c.err)
	}
	return newListPostings(c.ids...)
}

func (c *PostingsCloner) NumPostings() int {
	return len(c.ids)
}
