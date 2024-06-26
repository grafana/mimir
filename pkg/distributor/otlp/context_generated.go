// Code generated from Prometheus sources - DO NOT EDIT.

// Copyright 2024 The Prometheus Authors
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
package otlp

import "context"

// everyNTimes supports checking for context error every n times.
type everyNTimes struct {
	n   int
	i   int
	err error
}

// checkContext calls ctx.Err() every e.n times and returns an eventual error.
func (e *everyNTimes) checkContext(ctx context.Context) error {
	if e.err != nil {
		return e.err
	}

	e.i++
	if e.i >= e.n {
		e.i = 0
		e.err = ctx.Err()
	}

	return e.err
}
