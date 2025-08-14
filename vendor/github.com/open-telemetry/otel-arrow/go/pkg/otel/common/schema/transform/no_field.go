/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transform

import "github.com/apache/arrow-go/v18/arrow"

// NoField is a FieldTransform that returns nil, so in practice it removes the
// field.
type NoField struct{}

func (t *NoField) Transform(_ *arrow.Field) *arrow.Field {
	return nil
}

func (t *NoField) RevertCounters() {}

func (t *NoField) Path() string {
	return "undefined"
}
