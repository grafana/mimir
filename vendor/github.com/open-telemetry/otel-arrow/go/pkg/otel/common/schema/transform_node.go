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
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"

	cfg "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/events"
	transform2 "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/transform"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/update"
)

// FieldTransform is an interface to apply a transformation to a field.
type FieldTransform interface {
	Transform(field *arrow.Field) *arrow.Field
	RevertCounters()
}

// TransformNode is a node in a transformation tree.
// It can be a leaf node or a node with children.
type TransformNode struct {
	name       string
	path       string
	transforms []FieldTransform
	Children   []*TransformNode
}

// NewTransformTreeFrom creates a transformation tree from a prototype schema.
// The dictIndexType and the field metadata are used to transform the prototype
// schema into the target schema.
//
// Optional fields:
// By default all fields marked as optional in the prototype schema are removed
// from the target schema. This behavior can be changed if data is available for
// this field.
//
// Dictionary fields:
// By default all fields marked as dictionary fields in the prototype schema are
// converted to their dictionary representation. This behavior can be changed if
// the number of unique values is higher than the size of dictIndexType.
// If dictIndexType is nil, then fields marked as dictionary fields are not
// converted to their dictionary representation.
func NewTransformTreeFrom(
	prototype *arrow.Schema,
	dictConfig *cfg.Dictionary,
	schemaUpdateRequest *update.SchemaUpdateRequest,
	events *events.Events,
) (*TransformNode, map[string]*transform2.DictionaryField) {
	dictTransformNodes := make(map[string]*transform2.DictionaryField)

	protoFields := prototype.Fields()
	rootTNode := TransformNode{Children: make([]*TransformNode, 0, len(protoFields))}

	for i := 0; i < len(protoFields); i++ {
		rootTNode.Children = append(rootTNode.Children, newTransformNodeFrom(
			"",
			&protoFields[i],
			dictConfig,
			dictTransformNodes,
			schemaUpdateRequest,
			events,
		))
	}

	return &rootTNode, dictTransformNodes
}

func newTransformNodeFrom(
	path string,
	prototype *arrow.Field,
	dictConfig *cfg.Dictionary,
	dictTransformNodes map[string]*transform2.DictionaryField,
	schemaUpdateRequest *update.SchemaUpdateRequest,
	events *events.Events,
) *TransformNode {
	var transforms []FieldTransform

	// Update the current path.
	if len(path) > 0 {
		path += "."
	}
	path += prototype.Name

	// Check if the field is optional and if so, remove it by emitting a
	// NoField transformation.
	metadata := prototype.Metadata
	keyIdx := metadata.FindKey(OptionalKey)
	if keyIdx != -1 || prototype.Nullable {
		transforms = append(transforms, &transform2.NoField{})
	}

	// Check if the field is a dictionary field and if so, convert it to its
	// dictionary representation by emitting a DictionaryField transformation.
	keyIdx = metadata.FindKey(DictionaryKey)
	if keyIdx != -1 {
		initialDictIndexWidth := metadata.Values()[keyIdx]
		var localDictConfig *cfg.Dictionary

		switch initialDictIndexWidth {
		case "8":
			localDictConfig = cfg.NewDictionaryFrom(math.MaxUint8, dictConfig)
		case "16":
			localDictConfig = cfg.NewDictionaryFrom(math.MaxUint16, dictConfig)
		default:
			localDictConfig = dictConfig
		}

		dictId := strconv.Itoa(len(dictTransformNodes))
		dictTransform := transform2.NewDictionaryField(path, dictId, localDictConfig, schemaUpdateRequest, events)
		dictTransformNodes[dictId] = dictTransform
		transforms = append(transforms, dictTransform)
	}

	// If no transformation was added, then add an Identity transformation.
	if len(transforms) == 0 {
		transforms = append(transforms, transform2.NewIdentityField(path))
	}

	node := TransformNode{name: prototype.Name, path: path, transforms: transforms}

	switch dt := prototype.Type.(type) {
	case *arrow.DictionaryType:
		dictId := strconv.Itoa(len(dictTransformNodes))
		dictTransform := transform2.NewDictionaryField(path, dictId, dictConfig, schemaUpdateRequest, events)
		dictTransformNodes[dictId] = dictTransform
		node.transforms = append(node.transforms, dictTransform)
	case *arrow.StructType:
		node.Children = make([]*TransformNode, 0, len(dt.Fields()))
		for _, child := range prototype.Type.(*arrow.StructType).Fields() {
			node.Children = append(node.Children, newTransformNodeFrom(
				path,
				&child,
				dictConfig,
				dictTransformNodes,
				schemaUpdateRequest,
				events,
			))
		}
	case *arrow.ListType:
		elemField := dt.ElemField()
		node.Children = make([]*TransformNode, 0, 1)
		node.Children = append(node.Children, newTransformNodeFrom(
			path,
			&elemField,
			dictConfig,
			dictTransformNodes,
			schemaUpdateRequest,
			events,
		))
	case arrow.UnionType:
		node.Children = make([]*TransformNode, 0, len(dt.Fields()))
		for _, child := range dt.Fields() {
			node.Children = append(node.Children, newTransformNodeFrom(
				path,
				&child,
				dictConfig,
				dictTransformNodes,
				schemaUpdateRequest,
				events,
			))
		}
	case *arrow.MapType:
		node.Children = make([]*TransformNode, 0, 2)
		keyField := dt.KeyField()
		node.Children = append(node.Children, newTransformNodeFrom(
			path,
			&keyField,
			dictConfig,
			dictTransformNodes,
			schemaUpdateRequest,
			events,
		))

		valueField := dt.ItemField()
		node.Children = append(node.Children, newTransformNodeFrom(
			path,
			&valueField,
			dictConfig,
			dictTransformNodes,
			schemaUpdateRequest,
			events,
		))
	}

	return &node
}

// RemoveOptional removes all transformations that are marked as optional.
// This will take effect on the next cycle of appending data.
//
// To avoid data loss, the methods `AppendNonZero[XYZ]` should be called
// again after calling this method.
func (t *TransformNode) RemoveOptional() {
	n := 0

	for _, transform := range t.transforms {
		if _, ok := transform.(*transform2.NoField); !ok {
			t.transforms[n] = transform
			n++
		}
	}

	if n == 0 {
		t.transforms = []FieldTransform{transform2.NewIdentityField(t.path)}
	} else {
		t.transforms = t.transforms[:n]
	}
}

func (t *TransformNode) RevertCounters() {
	for _, transform := range t.transforms {
		transform.RevertCounters()
	}
	for _, child := range t.Children {
		child.RevertCounters()
	}
}

func (t *TransformNode) Path() string {
	return t.path
}
