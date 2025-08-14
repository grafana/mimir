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

package schema

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

// Metadata constants used to mark fields as optional or dictionary.

type MetadataKey int

const (
	Optional MetadataKey = iota
	Dictionary8
	Dictionary16
	DeltaEncoding

	OptionalKey   = "#optional"
	DictionaryKey = "#dictionary"
)

var (
	ErrSchemaNotUpToDate = errors.New("schema not up to date")
)

// Metadata returns a map of Arrow metadata for the given metadata keys.
func Metadata(keys ...MetadataKey) arrow.Metadata {
	m := make(map[string]string, len(keys))
	for _, key := range keys {
		switch key {
		case Optional:
			m[OptionalKey] = "true"
		case Dictionary8:
			m[DictionaryKey] = "8"
		case Dictionary16:
			m[DictionaryKey] = "16"
		}
	}
	return arrow.MetadataFrom(m)
}

// NewSchemaFrom creates a new schema from a prototype schema and a transformation tree.
func NewSchemaFrom(prototype *arrow.Schema, transformTree *TransformNode, md map[string]string) *arrow.Schema {
	protoFields := prototype.Fields()
	fields := make([]arrow.Field, 0, len(protoFields))

	for i := 0; i < len(protoFields); i++ {
		field := NewFieldFrom(&protoFields[i], transformTree.Children[i])
		if field != nil {
			fields = append(fields, *NewFieldFrom(&protoFields[i], transformTree.Children[i]))
		}
	}

	metadata := cleanMetadata(prototype.Metadata(), md)

	return arrow.NewSchema(fields, &metadata)

}

// NewFieldFrom creates a new field from a prototype field and a transformation tree.
func NewFieldFrom(prototype *arrow.Field, transformNode *TransformNode) *arrow.Field {
	field := prototype

	// remove metadata keys that are only used to specify transformations.

	// apply transformations to the current prototype field.
	// if a transformation returns nil, the field is removed.
	for _, t := range transformNode.transforms {
		field = t.Transform(field)
		if field == nil {
			return nil
		}
	}
	metadata := cleanMetadata(field.Metadata, nil)

	switch dt := field.Type.(type) {
	case *arrow.StructType:
		oldFields := dt.Fields()
		newFields := make([]arrow.Field, 0, len(oldFields))

		for i := 0; i < len(oldFields); i++ {
			newField := NewFieldFrom(&oldFields[i], transformNode.Children[i])
			if newField != nil {
				newFields = append(newFields, *newField)
			}
		}

		return &arrow.Field{Name: field.Name, Type: arrow.StructOf(newFields...), Nullable: field.Nullable, Metadata: metadata}
	case *arrow.ListType:
		elemField := dt.ElemField()
		newField := NewFieldFrom(&elemField, transformNode.Children[0])
		return &arrow.Field{Name: field.Name, Type: arrow.ListOf(newField.Type), Nullable: field.Nullable, Metadata: metadata}
	case arrow.UnionType:
		oldFields := dt.Fields()
		oldTypeCodes := dt.TypeCodes()
		newFields := make([]arrow.Field, 0, len(oldFields))
		newTypeCodes := make([]int8, 0, len(oldTypeCodes))

		for i := 0; i < len(oldFields); i++ {
			newField := NewFieldFrom(&oldFields[i], transformNode.Children[i])
			if newField != nil {
				newFields = append(newFields, *newField)
				newTypeCodes = append(newTypeCodes, oldTypeCodes[i])
			}
		}

		switch dt.(type) {
		case *arrow.SparseUnionType:
			return &arrow.Field{Name: field.Name, Type: arrow.SparseUnionOf(newFields, newTypeCodes), Nullable: field.Nullable, Metadata: metadata}
		case *arrow.DenseUnionType:
			return &arrow.Field{Name: field.Name, Type: arrow.DenseUnionOf(newFields, newTypeCodes), Nullable: field.Nullable, Metadata: metadata}
		default:
			panic("unknown union type")
		}
	case *arrow.MapType:
		keyField := dt.KeyField()
		newKeyField := NewFieldFrom(&keyField, transformNode.Children[0])
		valueField := dt.ItemField()
		newValueField := NewFieldFrom(&valueField, transformNode.Children[1])

		if newKeyField == nil || newValueField == nil {
			return nil
		}
		return &arrow.Field{Name: field.Name, Type: arrow.MapOfWithMetadata(
			newKeyField.Type, newKeyField.Metadata,
			newValueField.Type, newValueField.Metadata), Nullable: field.Nullable, Metadata: metadata}
	default:
		return &arrow.Field{Name: field.Name, Type: field.Type, Nullable: field.Nullable, Metadata: metadata}
	}
}

func cleanMetadata(metadata arrow.Metadata, otherMetadata map[string]string) arrow.Metadata {
	keyCount := len(metadata.Keys())
	valueCount := len(metadata.Values())

	if otherMetadata != nil {
		keyCount += len(otherMetadata)
		valueCount += len(otherMetadata)
	}

	keys := make([]string, 0, keyCount)
	values := make([]string, 0, valueCount)

	for i, key := range metadata.Keys() {
		if key == OptionalKey || key == DictionaryKey {
			continue
		}
		keys = append(keys, key)
		values = append(values, metadata.Values()[i])
	}

	if otherMetadata != nil {
		for key, value := range otherMetadata {
			keys = append(keys, key)
			values = append(values, value)
		}
	}

	return arrow.NewMetadata(keys, values)
}
