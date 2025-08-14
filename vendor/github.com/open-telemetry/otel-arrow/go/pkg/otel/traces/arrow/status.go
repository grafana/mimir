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
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// StatusDT is the Arrow Data Type describing a span status.
var (
	StatusDT = arrow.StructOf([]arrow.Field{
		{Name: constants.StatusCode, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.StatusMessage, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
	}...)
)

type StatusBuilder struct {
	released bool

	builder *builder.StructBuilder // `status` builder

	scb *builder.Int32Builder  // status `code` builder
	smb *builder.StringBuilder // status `message` builder
}

func StatusBuilderFrom(sb *builder.StructBuilder) *StatusBuilder {
	return &StatusBuilder{
		released: false,
		builder:  sb,
		scb:      sb.Int32Builder(constants.StatusCode),
		smb:      sb.StringBuilder(constants.StatusMessage),
	}
}

// Append appends a new span status to the builder.
func (b *StatusBuilder) Append(status ptrace.Status) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(status, func() error {
		b.scb.AppendNonZero(int32(status.Code()))
		b.smb.AppendNonEmpty(status.Message())
		return nil
	})
}

// Build builds the span status array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *StatusBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *StatusBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
