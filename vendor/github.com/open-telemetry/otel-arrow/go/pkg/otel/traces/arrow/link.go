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

// Links are represented as Arrow records.
//
// A link accumulator is used to collect of the links across all spans, and
// once the entire trace is processed, the links are being globally sorted and
// written to the Arrow record batch. This process improves the compression
// ratio of the Arrow record batch.

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// LinkSchema is the Arrow Data Type describing a link (as a related record
	// to the main trace record).
	LinkSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.DeltaEncoding), Nullable: true},
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.TraceState, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	}, nil)
)

type (
	// LinkBuilder is an Arrow builder for Link records.
	LinkBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib   *builder.Uint32DeltaBuilder     // `id` builder
		pib  *builder.Uint16Builder          // `parent_id` builder
		tib  *builder.FixedSizeBinaryBuilder // `trace_id` builder
		sib  *builder.FixedSizeBinaryBuilder // `span_id` builder
		tsb  *builder.StringBuilder          // `trace_state` builder
		dacb *builder.Uint32Builder          // `dropped_attributes_count` builder

		accumulator *LinkAccumulator
		attrsAccu   *acommon.Attributes32Accumulator

		config *LinkConfig
	}

	// Link is an internal representation of a link used by the
	// LinkAccumulator.
	Link struct {
		ParentID               uint16
		TraceID                [16]byte
		SpanID                 [8]byte
		TraceState             string
		Attributes             pcommon.Map
		DroppedAttributesCount uint32
	}

	// LinkAccumulator is an accumulator for links that is used to sort links
	// globally in order to improve compression.
	LinkAccumulator struct {
		groupCount uint16
		links      []*Link
		sorter     LinkSorter
	}

	LinkSorter interface {
		Sort(links []*Link) []string
		Encode(parentID uint16, link *Link) uint16
		Reset()
	}

	LinksByNothing         struct{}
	LinksByTraceIdParentId struct {
		prevParentID uint16
		prevLink     *Link
	}
)

func NewLinkBuilder(rBuilder *builder.RecordBuilderExt, conf *LinkConfig) *LinkBuilder {
	b := &LinkBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewLinkAccumulator(conf.Sorter),
		config:      conf,
	}

	b.init()

	return b
}

func (b *LinkBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the links are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)
	b.tib = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
	b.sib = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.tsb = b.builder.StringBuilder(constants.TraceState)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
}

func (b *LinkBuilder) SetAttributesAccumulator(accu *acommon.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *LinkBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *LinkBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *LinkBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *LinkBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *LinkBuilder) PayloadType() *acommon.PayloadType {
	return acommon.PayloadTypes.Link
}

func (b *LinkBuilder) Accumulator() *LinkAccumulator {
	return b.accumulator
}

func (b *LinkBuilder) Build() (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		b.attrsAccu.Reset()
		record, err = b.TryBuild(b.attrsAccu)
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}

	return record, werror.Wrap(err)
}

func (b *LinkBuilder) TryBuild(attrsAccu *acommon.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.sorter.Reset()
	sortingColumns := b.accumulator.sorter.Sort(b.accumulator.links)
	b.builder.AddMetadata(constants.SortingColumns, strings.Join(sortingColumns, ","))

	linkID := uint32(0)

	b.builder.Reserve(len(b.accumulator.links))

	for _, link := range b.accumulator.links {
		if link.Attributes.Len() == 0 {
			b.ib.AppendNull()
		} else {
			b.ib.Append(linkID)

			// Attributes
			err = attrsAccu.Append(linkID, link.Attributes)
			if err != nil {
				return
			}

			linkID++
		}

		b.pib.Append(b.accumulator.sorter.Encode(link.ParentID, link))
		b.tib.Append(link.TraceID[:])
		b.sib.Append(link.SpanID[:])
		b.tsb.AppendNonEmpty(link.TraceState)

		b.dacb.AppendNonZero(link.DroppedAttributesCount)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

// Release releases the memory allocated by the builder.
func (b *LinkBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

// NewLinkAccumulator creates a new LinkAccumulator.
func NewLinkAccumulator(sorter LinkSorter) *LinkAccumulator {
	return &LinkAccumulator{
		groupCount: 0,
		links:      make([]*Link, 0),
		sorter:     sorter,
	}
}

func (a *LinkAccumulator) IsEmpty() bool {
	return len(a.links) == 0
}

// Append appends a new link to the builder.
func (a *LinkAccumulator) Append(spanID uint16, links ptrace.SpanLinkSlice) error {
	if a.groupCount == math.MaxUint16 {
		panic("The maximum number of group of links has been reached (max is uint16).")
	}

	if links.Len() == 0 {
		return nil
	}

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		a.links = append(a.links, &Link{
			ParentID:               spanID,
			TraceID:                link.TraceID(),
			SpanID:                 link.SpanID(),
			TraceState:             link.TraceState().AsRaw(),
			Attributes:             link.Attributes(),
			DroppedAttributesCount: link.DroppedAttributesCount(),
		})
	}

	a.groupCount++

	return nil
}

func (a *LinkAccumulator) Reset() {
	a.groupCount = 0
	a.links = a.links[:0]
}

// No sorting
// ==========

func UnsortedLinks() *LinksByNothing {
	return &LinksByNothing{}
}

func (s *LinksByNothing) Sort(_ []*Link) []string {
	return []string{}
}

func (s *LinksByNothing) Encode(parentID uint16, _ *Link) uint16 {
	return parentID
}

func (s *LinksByNothing) Reset() {}

// Sorts by TraceID, ParentID
// ==========================

func SortLinksByTraceIdParentId() *LinksByTraceIdParentId {
	return &LinksByTraceIdParentId{}
}

func (s *LinksByTraceIdParentId) Sort(links []*Link) []string {
	sort.Slice(links, func(i, j int) bool {
		linkI := links[i]
		linkJ := links[j]

		cmp := bytes.Compare(linkI.TraceID[:], linkJ.TraceID[:])
		if cmp == 0 {
			return linkI.ParentID < linkJ.ParentID
		} else {
			return cmp == -1
		}
	})
	return []string{constants.TraceId, constants.ParentID}
}

func (s *LinksByTraceIdParentId) Encode(parentID uint16, link *Link) uint16 {
	if s.prevLink == nil {
		s.prevLink = link
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(link) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevLink = link
		s.prevParentID = parentID
		return parentID
	}
}

func (s *LinksByTraceIdParentId) Reset() {
	s.prevParentID = 0
	s.prevLink = nil
}

func (s *LinksByTraceIdParentId) IsSameGroup(link *Link) bool {
	if s.prevLink == nil {
		return false
	}

	return bytes.Equal(s.prevLink.TraceID[:], link.TraceID[:])
}
