// Copyright 2023 Grafana Labs
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

//go:build stringlabels

// Split out function which needs to be coded differently for stringlabels case.

package tsdb

import "strings"

func (sw *symbolsBatcher) addSymbol(sym string) error {
	if _, found := sw.buffer[sym]; !found {
		sym = strings.Clone(sym) // So we don't retain reference to the entire labels block.
		sw.buffer[sym] = struct{}{}
	}
	return sw.flushSymbols(false)
}
