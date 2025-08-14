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

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// ScopeDT is the Arrow Data Type describing a scope.
var (
	ScopeDT = arrow.StructOf([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: acommon.Metadata(acommon.DeltaEncoding), Nullable: true},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: acommon.Metadata(acommon.Dictionary8), Nullable: true},
		{Name: constants.Version, Type: arrow.BinaryTypes.String, Metadata: acommon.Metadata(acommon.Dictionary8), Nullable: true},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	}...)
)

type ScopeBuilder struct {
	released bool
	builder  *builder.StructBuilder
	nb       *builder.StringBuilder      // Name builder
	vb       *builder.StringBuilder      // Version builder
	aib      *builder.Uint16DeltaBuilder // attributes id builder
	dacb     *builder.Uint32Builder      // Dropped attributes count builder
}

// ScopeBuilderFrom creates a new instrumentation scope array builder from an existing struct builder.
func ScopeBuilderFrom(sb *builder.StructBuilder) *ScopeBuilder {
	aib := sb.Uint16DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	// We are enforcing this constraint to make sure that the delta encoding
	// will always be used efficiently.
	aib.SetMaxDelta(1)
	return &ScopeBuilder{
		released: false,
		builder:  sb,
		nb:       sb.StringBuilder(constants.Name),
		vb:       sb.StringBuilder(constants.Version),
		aib:      aib,
		dacb:     sb.Uint32Builder(constants.DroppedAttributesCount),
	}
}

func (b *ScopeBuilder) Append(scopeID int64, scope pcommon.InstrumentationScope) error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(scope, func() error {
		b.nb.AppendNonEmpty(scope.Name())
		b.vb.AppendNonEmpty(scope.Version())
		b.aib.Append(uint16(scopeID))
		b.dacb.AppendNonZero(scope.DroppedAttributesCount())
		return nil
	})
}

// Build builds the instrumentation scope array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *ScopeBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
