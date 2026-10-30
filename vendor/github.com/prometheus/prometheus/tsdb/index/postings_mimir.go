// Copyright 2026 Grafana Labs
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

package index

import (
	"math/rand/v2"

	"github.com/prometheus/prometheus/storage"
)

// PostingsLength returns the number of series references in the postings list for the label pair name=value.
func (p *MemPostings) PostingsLength(name, value string) int {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	return len(p.m[name][value])
}

// AppendRandomPostingsRefs appends count series references from random positions in the postings list
// for the label pair name=value to buffer, and returns the extended buffer.
// Each position is chosen independently, so the same reference can occur more than once.
// It appends nothing if the label pair does not exist.
func (p *MemPostings) AppendRandomPostingsRefs(name, value string, count int, buffer []storage.SeriesRef) []storage.SeriesRef {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	list := p.m[name][value]
	if len(list) == 0 {
		return buffer
	}
	for range count {
		buffer = append(buffer, list[rand.IntN(len(list))])
	}
	return buffer
}
