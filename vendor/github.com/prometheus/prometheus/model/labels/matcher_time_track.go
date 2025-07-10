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

package labels

import (
	"time"

	"go.uber.org/atomic"
)

// NewMatcherWithTimeTracker returns a matcher which can track the time spent running regular expression matchers.
// duration is incremented when the MatchType is MatchRegexp or MatchNotRegexp.
// duration is incremented every 64 Matcher.Matches() invocations and multiplied by 64;
// the assumption is that all previous 63 invocations took the same time.
func NewMatcherWithTimeTracker(t MatchType, n, v string, duration *atomic.Duration) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := NewFastRegexMatcherWithTimeTracker(v, duration)
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// NewFastRegexMatcherWithTimeTracker returns a matcher which will track the time spent running the matcher.
// duration is incremented every 64 Matcher.Matches() invocations and multiplied by 64;
// the assumption is that all previous 63 invocations took the same time.
func NewFastRegexMatcherWithTimeTracker(regex string, duration *atomic.Duration) (*FastRegexMatcher, error) {
	m, err := NewFastRegexMatcher(regex)
	if err != nil {
		return nil, err
	}
	withDifferentObserver := *m
	sampler := atomic.NewInt64(-1)
	oldMatchString := m.matchString
	withDifferentObserver.matchString = func(s string) bool {
		const sampleRate = 64
		if tick := sampler.Inc(); tick%sampleRate == 0 {
			defer func(start time.Time) {
				d := time.Since(start)
				if tick != 0 {
					d *= sampleRate
				}
				duration.Add(d)
			}(time.Now())
		}
		return oldMatchString(s)
	}
	return &withDifferentObserver, nil
}
