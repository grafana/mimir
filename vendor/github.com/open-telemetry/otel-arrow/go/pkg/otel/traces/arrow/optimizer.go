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

// A trace optimizer used to regroup spans by resource and scope.

import (
	"bytes"
	"fmt"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
)

type (
	TracesOptimizer struct {
		sorter SpanSorter
	}

	TracesOptimized struct {
		Spans []*FlattenedSpan
	}

	FlattenedSpan struct {
		// Resource span section.
		ResourceSpanID    string
		Resource          pcommon.Resource
		ResourceSchemaUrl string

		// Scope span section.
		ScopeSpanID    string
		Scope          pcommon.InstrumentationScope
		ScopeSchemaUrl string

		// Span section.
		Span ptrace.Span
	}

	SpanSorter interface {
		Sort(spans []*FlattenedSpan)
	}

	SpansByNothing                                            struct{}
	SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName struct{}
	SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId struct{}
	SpansByResourceSpanIdScopeSpanIdNameStartTimestamp        struct{}
	SpansByResourceSpanIdScopeSpanIdNameTraceId               struct{}
	SpansByResourceSpanIdScopeSpanIdTraceIdName               struct{}
	SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp struct{}
)

func NewTracesOptimizer(sorter SpanSorter) *TracesOptimizer {
	return &TracesOptimizer{
		sorter: sorter,
	}
}

func (t *TracesOptimizer) Optimize(traces ptrace.Traces) *TracesOptimized {
	tracesOptimized := &TracesOptimized{
		Spans: make([]*FlattenedSpan, 0),
	}

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)
		resource := resSpan.Resource()
		resourceSchemaUrl := resSpan.SchemaUrl()
		resSpanID := otlp.ResourceID(resource, resourceSchemaUrl)

		scopeSpans := resSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			scope := scopeSpan.Scope()
			scopeSchemaUrl := scopeSpan.SchemaUrl()
			scopeSpanId := otlp.ScopeID(scope, scopeSchemaUrl)

			spans := scopeSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				tracesOptimized.Spans = append(tracesOptimized.Spans, &FlattenedSpan{
					ResourceSpanID:    resSpanID,
					Resource:          resource,
					ResourceSchemaUrl: resourceSchemaUrl,
					ScopeSpanID:       scopeSpanId,
					Scope:             scope,
					ScopeSchemaUrl:    scopeSchemaUrl,
					Span:              span,
				})
			}
		}
	}

	t.sorter.Sort(tracesOptimized.Spans)

	return tracesOptimized
}

func FindOrderByFunc(orderBy config.OrderSpanBy) SpanSorter {
	switch orderBy {
	case config.OrderSpanByNothing:
		return &SpansByNothing{}
	case config.OrderSpanByNameTraceID:
		return &SpansByResourceSpanIdScopeSpanIdNameTraceId{}
	case config.OrderSpanByTraceIDName:
		return &SpansByResourceSpanIdScopeSpanIdTraceIdName{}
	case config.OrderSpanByNameStartTime:
		return &SpansByResourceSpanIdScopeSpanIdNameStartTimestamp{}
	case config.OrderSpanByNameTraceIdStartTime:
		return &SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp{}
	case config.OrderSpanByStartTimeTraceIDName:
		return &SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName{}
	case config.OrderSpanByStartTimeNameTraceID:
		return &SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId{}
	default:
		panic(fmt.Sprintf("unknown OrderSpanBy variant: %d", orderBy))
	}
}

// No sorting
// ==========

func UnsortedSpans() *SpansByNothing {
	return &SpansByNothing{}
}

func (s *SpansByNothing) Sort(_ []*FlattenedSpan) {
}

// Sorts spans by resource span id, scope span id, start timestamp, trace id, name
// ===============================================================================

func SortSpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName() *SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName {
	return &SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName{}
}

func (s *SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.StartTimestamp() == spanJ.Span.StartTimestamp() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])

					if cmp == 0 {
						return spanI.Span.Name() < spanJ.Span.Name()
					} else {
						return cmp < 0
					}
				} else {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId() *SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId {
	return &SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId{}
}

func (s *SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.StartTimestamp() == spanJ.Span.StartTimestamp() {
					if spanI.Span.Name() == spanJ.Span.Name() {
						var traceI [16]byte
						var traceJ [16]byte

						traceI = spanI.Span.TraceID()
						traceJ = spanJ.Span.TraceID()
						cmp := bytes.Compare(traceI[:], traceJ[:])
						return cmp < 0
					} else {
						return spanI.Span.Name() < spanJ.Span.Name()
					}
				} else {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameStartTimestamp() *SpansByResourceSpanIdScopeSpanIdNameStartTimestamp {
	return &SpansByResourceSpanIdScopeSpanIdNameStartTimestamp{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameStartTimestamp) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameTraceId() *SpansByResourceSpanIdScopeSpanIdNameTraceId {
	return &SpansByResourceSpanIdScopeSpanIdNameTraceId{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameTraceId) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])
					return cmp < 0
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdTraceIdName() *SpansByResourceSpanIdScopeSpanIdTraceIdName {
	return &SpansByResourceSpanIdScopeSpanIdTraceIdName{}
}

func (s *SpansByResourceSpanIdScopeSpanIdTraceIdName) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				var traceI [16]byte
				var traceJ [16]byte

				traceI = spanI.Span.TraceID()
				traceJ = spanJ.Span.TraceID()
				cmp := bytes.Compare(traceI[:], traceJ[:])

				if cmp == 0 {
					return spanI.Span.Name() < spanJ.Span.Name()
				} else {
					return cmp < 0
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp() *SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp {
	return &SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])
					if cmp == 0 {
						return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
					} else {
						return cmp < 0
					}
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}
