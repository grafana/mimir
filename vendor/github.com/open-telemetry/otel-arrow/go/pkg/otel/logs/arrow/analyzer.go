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
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/axiomhq/hyperloglog"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
)

// A log analyzer is a tool designed to generate statistics about the structure
// and content distribution of a stream of OpenTelemetry Protocol (OTLP) logs.
// By using the -stats flag in the benchmark tool, the results of this analysis
// can be conveniently displayed on the console to troubleshoot compression
// ratio issues.

type (
	LogsAnalyzer struct {
		LogRecordCount    int64
		ResourceLogsStats *ResourceLogsStats
	}

	ResourceLogsStats struct {
		TotalCount         int64
		Distribution       *hdrhistogram.Histogram
		ResLogsIDsDistinct *hyperloglog.Sketch
		ResourceStats      *carrow.ResourceStats
		ScopeLogsStats     *ScopeLogsStats
		SchemaUrlStats     *carrow.SchemaUrlStats
	}

	ScopeLogsStats struct {
		Distribution         *hdrhistogram.Histogram
		ScopeLogsIDsDistinct *hyperloglog.Sketch
		ScopeStats           *carrow.ScopeStats
		SchemaUrlStats       *carrow.SchemaUrlStats
		LogRecordStats       *LogRecordStats
	}

	LogRecordStats struct {
		TotalCount           int64
		Distribution         *hdrhistogram.Histogram
		TimeUnixNano         *carrow.TimestampStats
		ObservedTimeUnixNano *carrow.TimestampStats
		TraceID              *hyperloglog.Sketch
		SpanID               *hyperloglog.Sketch
		SeverityNumber       *hyperloglog.Sketch
		SeverityText         *carrow.StringStats
		Body                 *carrow.AnyValueStats
		Attributes           *carrow.AttributesStats
	}
)

func NewLogsAnalyzer() *LogsAnalyzer {
	return &LogsAnalyzer{
		ResourceLogsStats: &ResourceLogsStats{
			Distribution:       hdrhistogram.New(1, 1000000, 2),
			ResLogsIDsDistinct: hyperloglog.New16(),
			ResourceStats: &carrow.ResourceStats{
				AttributesStats: carrow.NewAttributesStats(),
			},
			ScopeLogsStats: &ScopeLogsStats{
				Distribution:         hdrhistogram.New(1, 1000000, 2),
				ScopeLogsIDsDistinct: hyperloglog.New16(),
				ScopeStats: &carrow.ScopeStats{
					AttributesStats: carrow.NewAttributesStats(),
					Name:            carrow.NewStringStats(),
					Version:         carrow.NewStringStats(),
				},
				LogRecordStats: NewLogRecordStats(),
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

func (t *LogsAnalyzer) Analyze(logs *LogsOptimized) {
	t.LogRecordCount++
	t.ResourceLogsStats.UpdateWith(logs)
}

func (t *LogsAnalyzer) ShowStats(indent string) {
	println()
	print(carrow.Green)
	fmt.Printf("%s%d ExportLogServiceRequest processed\n", indent, t.LogRecordCount)
	print(carrow.ColorReset)
	indent += "  "
	t.ResourceLogsStats.ShowStats(indent)
}

func (r *ResourceLogsStats) UpdateWith(logs *LogsOptimized) {
	// ToDo I will update this in a future PR. Only used for troubleshooting compression ratio issues.
	//resLogs := logs.ResourceLogs
	//
	//for ID := range logs.ResourceLogsIdx {
	//	r.ResLogsIDsDistinct.Insert([]byte(ID))
	//}
	//
	//r.TotalCount += int64(len(resLogs))
	//carrow.RequireNoError(r.Distribution.RecordValue(int64(len(resLogs))))
	//
	//for _, rs := range resLogs {
	//	r.ResourceStats.UpdateWith(rs.Resource)
	//	r.ScopeLogsStats.UpdateWith(rs.ScopeLogs, rs.ScopeLogsIdx)
	//	r.SchemaUrlStats.UpdateWith(rs.ResourceSchemaUrl)
	//}
}

func (r *ResourceLogsStats) ShowStats(indent string) {
	fmt.Printf("%s                                 |         Distribution per request        |\n", indent)
	print(carrow.Green)
	fmt.Printf("%sResourceLogs%s  |    Total|Distinct|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s              |%9d|%8d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent,
		r.TotalCount, r.ResLogsIDsDistinct.Estimate(), r.Distribution.Min(), r.Distribution.Max(), r.Distribution.Mean(), r.Distribution.StdDev(), r.Distribution.ValueAtQuantile(50), r.Distribution.ValueAtQuantile(99),
	)
	indent += "  "
	r.ResourceStats.ShowStats(indent)
	r.ScopeLogsStats.ShowStats(indent)
	r.SchemaUrlStats.ShowStats(indent)
}

//func (s *ScopeLogsStats) UpdateWith(scopeLogs []*ScopeLogGroup, scopeLogsIdx map[string]int) {
//	carrow.RequireNoError(s.Distribution.RecordValue(int64(len(scopeLogs))))
//
//	for ID := range scopeLogsIdx {
//		s.ScopeLogsIDsDistinct.Insert([]byte(ID))
//	}
//
//	for _, sl := range scopeLogs {
//		s.ScopeStats.UpdateWith(sl.Scope)
//		s.LogRecordStats.UpdateWith(sl)
//		s.SchemaUrlStats.UpdateWith(sl.ScopeSchemaUrl)
//	}
//}

func (s *ScopeLogsStats) ShowStats(indent string) {
	print(carrow.Green)
	fmt.Printf("%sScopeLogs%s  |Distinct|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s           |%8d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent,
		s.ScopeLogsIDsDistinct.Estimate(), s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	s.ScopeStats.ShowStats(indent + "  ")
	s.LogRecordStats.ShowStats(indent + "  ")
	s.SchemaUrlStats.ShowStats(indent + "  ")
}

func NewLogRecordStats() *LogRecordStats {
	return &LogRecordStats{
		Distribution:         hdrhistogram.New(0, 1000000, 2),
		Attributes:           carrow.NewAttributesStats(),
		TimeUnixNano:         carrow.NewTimestampStats(),
		ObservedTimeUnixNano: carrow.NewTimestampStats(),
		SpanID:               hyperloglog.New16(),
		TraceID:              hyperloglog.New16(),
		SeverityNumber:       hyperloglog.New16(),
		SeverityText:         carrow.NewStringStats(),
		Body:                 carrow.NewAnyValueStats(),
	}
}

//func (s *LogRecordStats) UpdateWith(sl *ScopeLogGroup) {
//	logs := sl.Logs
//	carrow.RequireNoError(s.Distribution.RecordValue(int64(len(logs))))
//
//	for _, logRecord := range logs {
//		s.TimeUnixNano.UpdateWith(logRecord.Timestamp())
//		s.ObservedTimeUnixNano.UpdateWith(logRecord.ObservedTimestamp())
//		s.Attributes.UpdateWith(logRecord.Attributes(), logRecord.DroppedAttributesCount())
//		s.SpanID.Insert([]byte(logRecord.SpanID().String()))
//		s.TraceID.Insert([]byte(logRecord.TraceID().String()))
//		s.SeverityNumber.Insert([]byte(strconv.Itoa(int(logRecord.SeverityNumber()))))
//		s.SeverityText.UpdateWith(logRecord.SeverityText())
//		s.Body.UpdateWith(logRecord.Body())
//	}
//
//	s.TotalCount += int64(len(logs))
//}

func (s *LogRecordStats) ShowStats(indent string) {
	print(carrow.Green)
	fmt.Printf("%sLogRecords%s |   Total|   Min|   Max|  Mean|  Stdev|   P50|   P99|\n", indent, carrow.ColorReset)
	fmt.Printf("%s           |%8d|%6d|%6d|%6.1f|%7.1f|%6d|%6d|\n", indent,
		s.TotalCount, s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	indent += "  "
	s.TimeUnixNano.ShowStats("TimeUnixNano", indent)
	s.ObservedTimeUnixNano.ShowStats("ObservedTimeUnixNano", indent)
	fmt.Printf("%s             |Distinct|   Total|%%Distinct|\n", indent)
	fmt.Printf("%s%sSpanID%s       |%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.SpanID.Estimate(), s.TotalCount, 100.0*float64(s.SpanID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s%sTraceID%s      |%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.TraceID.Estimate(), s.TotalCount, 100.0*float64(s.TraceID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s%sSeverityNumber%s|%8d|%8d|%8.1f%%|\n", indent, carrow.Green, carrow.ColorReset, s.SeverityNumber.Estimate(), s.TotalCount, 100.0*float64(s.SeverityNumber.Estimate())/float64(s.TotalCount))
	s.SeverityText.ShowStats("SeverityText", indent)
	s.Attributes.ShowStats(indent, "Attributes", carrow.Green)
	s.Body.ShowStats(indent, "Body", carrow.Green)
}
