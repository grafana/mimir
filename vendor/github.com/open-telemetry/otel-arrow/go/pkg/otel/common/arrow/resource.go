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

package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	schema "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// ResourceDT is the Arrow Data Type describing a resource.
var (
	ResourceDT = arrow.StructOf([]arrow.Field{
		{
			Name:     constants.ID,
			Type:     arrow.PrimitiveTypes.Uint16,
			Metadata: schema.Metadata(schema.DeltaEncoding),
			Nullable: true,
		},
		{
			Name:     constants.SchemaUrl,
			Type:     arrow.BinaryTypes.String,
			Metadata: schema.Metadata(schema.Dictionary8),
			Nullable: true,
		},
		{
			Name:     constants.DroppedAttributesCount,
			Type:     arrow.PrimitiveTypes.Uint32,
			Nullable: true,
		},
	}...)
)

// ResourceBuilder is an Arrow builder for resources.
type ResourceBuilder struct {
	released bool

	rBuilder *builder.RecordBuilderExt

	builder *builder.StructBuilder      // `resource` builder
	aib     *builder.Uint16DeltaBuilder // attributes id builder
	schb    *builder.StringBuilder      // `schema_url` builder
	dacb    *builder.Uint32Builder      // `dropped_attributes_count` field builder
}

// ResourceBuilderFrom creates a new resource builder from an existing struct builder.
func ResourceBuilderFrom(builder *builder.StructBuilder) *ResourceBuilder {
	aib := builder.Uint16DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	// We are enforcing this constraint to make sure that the delta encoding
	// will always be used efficiently.
	aib.SetMaxDelta(1)

	return &ResourceBuilder{
		released: false,
		builder:  builder,
		aib:      aib,
		schb:     builder.StringBuilder(constants.SchemaUrl),
		dacb:     builder.Uint32Builder(constants.DroppedAttributesCount),
	}
}

func (b *ResourceBuilder) Append(resID int64, resource pcommon.Resource, schemaUrl string) error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(resource, func() error {
		b.aib.Append(uint16(resID))
		b.schb.AppendNonEmpty(schemaUrl)
		b.dacb.AppendNonZero(resource.DroppedAttributesCount())
		return nil
	})
}

// Build builds the resource array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *ResourceBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
