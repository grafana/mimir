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

// ToDo the metrics analyzer is not complete yet. It is a work in progress and will be completed in a future PRs. The analyzer is only used for troubleshooting and debugging purposes.

import (
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/axiomhq/hyperloglog"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
)

type (
	MetricsAnalyzer struct {
		MetricCount          int64
		ResourceMetricsStats *ResourceMetricsStats
	}

	ResourceMetricsStats struct {
		TotalCount            int64
		Distribution          *hdrhistogram.Histogram
		ResMetricsIDsDistinct *hyperloglog.Sketch
		ResourceStats         *carrow.ResourceStats
		ScopeMetricsStats     *ScopeMetricsStats
		SchemaUrlStats        *carrow.SchemaUrlStats
	}

	ScopeMetricsStats struct {
		Distribution            *hdrhistogram.Histogram
		ScopeMetricsIDsDistinct *hyperloglog.Sketch
		ScopeStats              *carrow.ScopeStats
		SchemaUrlStats          *carrow.SchemaUrlStats
		MetricsStats            *MetricsStats
	}

	MetricsStats struct {
		TotalCount        int64
		Distribution      *hdrhistogram.Histogram
		Attributes        *carrow.AttributesStats
		SharedAttributes  *carrow.AttributesStats
		TimeIntervalStats *carrow.TimeIntervalStats
		Name              *carrow.StringStats
		SpanID            *hyperloglog.Sketch
		TraceID           *hyperloglog.Sketch
		ParentSpanID      *hyperloglog.Sketch
		Kind              *hyperloglog.Sketch
		TraceState        *hyperloglog.Sketch
		StatusStats       *carrow.StatusStats
	}
)

func NewMetricsAnalyzer() *MetricsAnalyzer {
	return &MetricsAnalyzer{
		ResourceMetricsStats: &ResourceMetricsStats{
			Distribution:          hdrhistogram.New(1, 1000000, 2),
			ResMetricsIDsDistinct: hyperloglog.New16(),
			ResourceStats: &carrow.ResourceStats{
				AttributesStats: carrow.NewAttributesStats(),
			},
			ScopeMetricsStats: &ScopeMetricsStats{
				Distribution:            hdrhistogram.New(1, 1000000, 2),
				ScopeMetricsIDsDistinct: hyperloglog.New16(),
				ScopeStats: &carrow.ScopeStats{
					AttributesStats: carrow.NewAttributesStats(),
					Name:            carrow.NewStringStats(),
					Version:         carrow.NewStringStats(),
				},
				MetricsStats: NewMetricsStats(),
				SchemaUrlStats: &carrow.SchemaUrlStats{
					SizeDistribution: hdrhistogram.New(1, 10000, 2),
				},
			},
			SchemaUrlStats: &carrow.SchemaUrlStats{
				SizeDistribution: hdrhistogram.New(1, 10000, 2),
			},
		},
	}
}

func (t *MetricsAnalyzer) Analyze(metrics *MetricsOptimized) {
	t.MetricCount++
	t.ResourceMetricsStats.UpdateWith(metrics)
}

func (t *MetricsAnalyzer) ShowStats(indent string) {
	println()
	print(carrow.Green)
	fmt.Printf("%s%d ExportMetricsServiceRequest processed\n", indent, t.MetricCount)
	print(carrow.ColorReset)
	indent += "  "
	t.ResourceMetricsStats.ShowStats(indent)
}

func (r *ResourceMetricsStats) UpdateWith(metrics *MetricsOptimized) {
	// ToDo adapt the analyzer to the new metrics format
	//resMetrics := metrics.ResourceMetrics
	//
	//for ID := range metrics.ResourceMetricsIdx {
	//	r.ResMetricsIDsDistinct.Insert([]byte(ID))
	//}
	//
	//r.TotalCount += int64(len(resMetrics))
	//carrow.RequireNoError(r.Distribution.RecordValue(int64(len(resMetrics))))
	//
	//for _, rs := range resMetrics {
	//	r.ResourceStats.UpdateWith(rs.Resource)
	//	r.ScopeMetricsStats.UpdateWith(rs.ScopeMetrics, rs.ScopeMetricsIdx)
	//	r.SchemaUrlStats.UpdateWith(rs.ResourceSchemaUrl)
	//}
}

func (r *ResourceMetricsStats) ShowStats(indent string) {
	fmt.Printf("%s                                 |         Distribution per request        |\n", indent)
	print(carrow.Green)
	fmt.Printf("%sResourceMetrics%s |    Total|Distinct|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s              |%9d|%8d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent,
		r.TotalCount, r.ResMetricsIDsDistinct.Estimate(), r.Distribution.Min(), r.Distribution.Max(), r.Distribution.Mean(), r.Distribution.StdDev(), r.Distribution.ValueAtQuantile(50), r.Distribution.ValueAtQuantile(99),
	)
	indent += "  "
	r.ResourceStats.ShowStats(indent)
	r.ScopeMetricsStats.ShowStats(indent)
	r.SchemaUrlStats.ShowStats(indent)
}

// ToDo adapt the analyzer to the new metrics format
//func (s *ScopeMetricsStats) UpdateWith(scopeMetrics []*ScopeMetricsGroup, scopeMetricsIdx map[string]int) {
//	carrow.RequireNoError(s.Distribution.RecordValue(int64(len(scopeMetrics))))
//
//	for ID := range scopeMetricsIdx {
//		s.ScopeMetricsIDsDistinct.Insert([]byte(ID))
//	}
//
//	for _, ss := range scopeMetrics {
//		s.ScopeStats.UpdateWith(ss.Scope)
//		s.MetricsStats.UpdateWith(ss)
//		s.SchemaUrlStats.UpdateWith(ss.ScopeSchemaUrl)
//	}
//}

func (s *ScopeMetricsStats) ShowStats(indent string) {
	print(carrow.Green)
	fmt.Printf("%sScopeMetrics%s |Distinct|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s           |%8d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent,
		s.ScopeMetricsIDsDistinct.Estimate(), s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	s.ScopeStats.ShowStats(indent + "  ")
	s.MetricsStats.ShowStats(indent + "  ")
	s.SchemaUrlStats.ShowStats(indent + "  ")
}

func NewMetricsStats() *MetricsStats {
	return &MetricsStats{
		Distribution:      hdrhistogram.New(0, 1000000, 2),
		Attributes:        carrow.NewAttributesStats(),
		SharedAttributes:  carrow.NewAttributesStats(),
		TimeIntervalStats: carrow.NewTimeIntervalStats(),
		SpanID:            hyperloglog.New16(),
		TraceID:           hyperloglog.New16(),
		ParentSpanID:      hyperloglog.New16(),
		Name:              carrow.NewStringStats(),
		Kind:              hyperloglog.New16(),
		TraceState:        hyperloglog.New16(),
		StatusStats:       carrow.NewStatusStats(),
	}
}

// ToDo adapt the analyzer to the new metrics format
//func (s *MetricsStats) UpdateWith(sm *ScopeMetricsGroup) {
//	metrics := sm.Metrics
//	carrow.RequireNoError(s.Distribution.RecordValue(int64(len(metrics))))

//sharedAttrs := pcommon.NewMap()
//sm.SharedData.sharedAttributes.CopyTo(sharedAttrs)
//s.SharedAttributes.UpdateWith(sharedAttrs, 0)
//
//s.TimeIntervalStats.UpdateWithSpans(metrics)
//
//for _, span := range metrics {
//	s.Attributes.UpdateWith(span.Attributes(), span.DroppedAttributesCount())
//	s.Name.UpdateWith(span.Name())
//	s.SpanID.Insert([]byte(span.SpanID().String()))
//	s.TraceID.Insert([]byte(span.TraceID().String()))
//	s.ParentSpanID.Insert([]byte(span.ParentSpanID().String()))
//	s.Kind.Insert([]byte(span.Kind().String()))
//	s.TraceState.Insert([]byte(span.TraceState().AsRaw()))
//	s.Events.UpdateWith(span.Events(), span.DroppedEventsCount(), sm.SharedData.sharedEventAttributes)
//	s.Links.UpdateWith(span.Links(), span.DroppedLinksCount(), sm.SharedData.sharedLinkAttributes)
//
//	b := make([]byte, 8)
//	binary.LittleEndian.PutUint64(b, uint64(span.DroppedEventsCount()))
//	s.DropEventsCount.Insert(b)
//	binary.LittleEndian.PutUint64(b, uint64(span.DroppedLinksCount()))
//	s.DropLinksCount.Insert(b)
//	s.StatusStats.UpdateWith(span.Status())
//}

//	s.TotalCount += int64(len(metrics))
//}

func (s *MetricsStats) ShowStats(indent string) {
	print(carrow.Green)
	fmt.Printf("%sSpans%s |   Total|   Min|   Max|  Mean|  Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s      |%8d|%6d|%6d|%6.1f|%7.1f|%6d|%6d|\n", indent,
		s.TotalCount, s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	indent += "  "
	s.TimeIntervalStats.ShowStats(indent)
	s.Name.ShowStats("Name", indent)
	fmt.Printf("%s             |Distinct|   Total|%%Distinct|\n", indent)
	fmt.Printf("%s%sSpanID%s       |%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.SpanID.Estimate(), s.TotalCount, 100.0*float64(s.SpanID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s%sTraceID%s      |%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.TraceID.Estimate(), s.TotalCount, 100.0*float64(s.TraceID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s%sParentSpanID%s |%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.ParentSpanID.Estimate(), s.TotalCount, 100.0*float64(s.ParentSpanID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s%sKind%s (Distinct=%d)\n", indent, carrow.Green, carrow.ColorReset, s.Kind.Estimate())
	fmt.Printf("%s%sTraceState%s (Distinct=%d)\n", indent, carrow.Green, carrow.ColorReset, s.TraceState.Estimate())

	s.Attributes.ShowStats(indent, "Attributes", carrow.Green)
	s.SharedAttributes.ShowStats(indent, "SharedAttributes", carrow.Cyan)
	s.StatusStats.ShowStats(indent)
}
