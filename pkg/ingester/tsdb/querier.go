// Copyright 2017 The Prometheus Authors
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

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

func labelValuesWithMatchers(r tsdb.IndexReader, name string, matchers ...*labels.Matcher) ([]string, error) {
	p, err := tsdb.PostingsForMatchers(r, matchers...)
	if err != nil {
		return nil, errors.Wrap(err, "fetching postings for matchers")
	}

	allValues, err := r.LabelValues(name)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching values of label %s", name)
	}
	valuesPostings := make([]index.Postings, len(allValues))
	for i, value := range allValues {
		valuesPostings[i], err = r.Postings(name, value)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching postings for %s=%q", name, value)
		}
	}
	indexes, err := index.FindIntersectingPostings(p, valuesPostings)
	if err != nil {
		return nil, errors.Wrap(err, "intersecting postings")
	}

	values := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		values = append(values, allValues[idx])
	}

	return values, nil
}

func labelNamesWithMatchers(r tsdb.IndexReader, matchers ...*labels.Matcher) ([]string, error) {
	p, err := r.PostingsForMatchers(false, matchers...)
	if err != nil {
		return nil, err
	}

	var postings []storage.SeriesRef
	for p.Next() {
		postings = append(postings, p.At())
	}
	if p.Err() != nil {
		return nil, errors.Wrapf(p.Err(), "postings for label names with matchers")
	}

	return r.LabelNamesFor(postings...)
}
