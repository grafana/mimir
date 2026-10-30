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

package tsdb

import "github.com/prometheus/prometheus/storage"

// LastSeriesRef returns the highest series reference that the head allocated.
// The head allocates a higher reference for each new series.
func (h *Head) LastSeriesRef() storage.SeriesRef {
	return storage.SeriesRef(h.lastSeriesID.Load())
}

// PostingsLength returns the number of series for the label pair name=value.
func (h *Head) PostingsLength(name, value string) int {
	return h.postings.PostingsLength(name, value)
}

// AppendRandomPostingsRefs appends count random series references for the label pair name=value to buffer.
// See index.MemPostings.AppendRandomPostingsRefs.
func (h *Head) AppendRandomPostingsRefs(name, value string, count int, buffer []storage.SeriesRef) []storage.SeriesRef {
	return h.postings.AppendRandomPostingsRefs(name, value, count, buffer)
}
