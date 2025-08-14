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

// IdentityField is a FieldTransform that returns a copy of the field.
type IdentityField struct {
	path string
}

func NewIdentityField(path string) *IdentityField {
	return &IdentityField{path: path}
}

func (t *IdentityField) Transform(field *arrow.Field) *arrow.Field {
	return &arrow.Field{Name: field.Name, Type: field.Type, Nullable: field.Nullable, Metadata: field.Metadata}
}

func (t *IdentityField) RevertCounters() {}

func (t *IdentityField) Path() string {
	return t.path
}
