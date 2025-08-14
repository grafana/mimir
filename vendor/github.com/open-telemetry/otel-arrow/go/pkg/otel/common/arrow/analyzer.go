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
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	ColorReset = "\033[0m"
	Green      = "\033[32m"
	Cyan       = "\033[36m"
	Grey       = "\033[90m"
)

type ResourceStats struct {
	TotalCount int64
	Missing    int64

	AttributesStats *AttributesStats
}

type ScopeStats struct {
	TotalCount int64
	Missing    int64

	AttributesStats *AttributesStats

	Name    *StringStats
	Version *StringStats
}

type StatusStats struct {
	TotalCount             int64
	Missing                int64
	CodeDistinctValue      *hyperloglog.Sketch
	MissingCode            int64
	MessageDistincValue    *hyperloglog.Sketch
	MessageLenDistribution *hdrhistogram.Histogram
	MissingMessage         int64
}

func (s *ScopeStats) UpdateWith(scope pcommon.InstrumentationScope) {
	s.TotalCount++
	s.AttributesStats.UpdateWith(scope.Attributes(), scope.DroppedAttributesCount())
}

func (s *ScopeStats) ShowStats(indent string) {
	if !s.Name.IsPresent() && !s.Version.IsPresent() && !s.AttributesStats.IsPresent() {
		fmt.Printf("%s%sNo Scope%s\n", indent, Grey, ColorReset)
		return
	}

	print(Green)
	fmt.Printf("%sScope%s (Missing=%d)\n", indent, ColorReset, s.Missing)
	indent += "  "
	s.Name.ShowStats("Name", indent)
	s.Version.ShowStats("Version", indent)
	s.AttributesStats.ShowStats(indent, "Attributes", Green)
}

func (r *ResourceStats) UpdateWith(res pcommon.Resource) {
	attrs := res.Attributes()
	dac := res.DroppedAttributesCount()

	r.TotalCount++

	if attrs.Len() == 0 && dac == 0 {
		r.Missing++
	}

	r.AttributesStats.UpdateWith(attrs, dac)
}

func (r *ResourceStats) ShowStats(indent string) {
	print(Green)
	fmt.Printf("%sResource%s (Missing=%d)\n", indent, ColorReset, r.Missing)
	r.AttributesStats.ShowStats(indent+"  ", "Attributes", Green)
}

func (s *StatusStats) UpdateWith(status ptrace.Status) {
	code := status.Code()
	msg := status.Message()

	if code == ptrace.StatusCodeUnset {
		s.MissingCode++
	} else {
		s.CodeDistinctValue.Insert([]byte(code.String()))
	}

	if len(msg) == 0 {
		s.MissingMessage++
	} else {
		s.MessageDistincValue.Insert([]byte(msg))
		RequireNoError(s.MessageLenDistribution.RecordValue(int64(len(msg))))
	}

	if code == ptrace.StatusCodeUnset && len(msg) == 0 {
		s.Missing++
		return
	}

	s.TotalCount++
}

func (s *StatusStats) ShowStats(indent string) {
	if s.TotalCount == 0 {
		print(Grey)
		fmt.Printf("%sNo Status%s\n", indent, ColorReset)
		return
	}

	print(Green)
	fmt.Printf("%sStatus%s (Missing=%d)\n", indent, ColorReset, s.Missing)

	indent = indent + "  "

	print(Green)
	fmt.Printf("%sCode%s |Missing|Distinct|\n", indent, ColorReset)
	fmt.Printf("%s     |%7d|%8d|\n", indent, s.MissingCode, s.CodeDistinctValue.Estimate())

	if s.MessageLenDistribution.TotalCount() == 0 {
		fmt.Printf("%s%sNo message%s\n", indent, Grey, ColorReset)
	} else {
		print(Green)
		fmt.Printf("%sMessage%s |Missing|Distinct|Len Min|Len Max|Len Mean|Len Stdev|Len P50|Len P99|\n", indent, ColorReset)
		fmt.Printf("%s        |%7d|%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indent, s.MissingMessage, s.MessageDistincValue.Estimate(), s.MessageLenDistribution.Min(), s.MessageLenDistribution.Max(), s.MessageLenDistribution.Mean(), s.MessageLenDistribution.StdDev(), s.MessageLenDistribution.ValueAtQuantile(50), s.MessageLenDistribution.ValueAtQuantile(99))
	}
}

type AnyValueStats struct {
	TotalCount int64
	Missing    int64

	// Attribute type distribution
	I64TypeDistribution    *hdrhistogram.Histogram
	F64TypeDistribution    *hdrhistogram.Histogram
	BoolTypeDistribution   *hdrhistogram.Histogram
	StringTypeDistribution *hdrhistogram.Histogram
	BinaryTypeDistribution *hdrhistogram.Histogram
	ListTypeDistribution   *hdrhistogram.Histogram
	MapTypeDistribution    *hdrhistogram.Histogram

	// Attribute distinct values per type
	I64DistinctValue    *hyperloglog.Sketch
	F64DistinctValue    *hyperloglog.Sketch
	StringDistinctValue *hyperloglog.Sketch
	BinaryDistinctValue *hyperloglog.Sketch

	// Content length distribution
	StringLenDistribution *hdrhistogram.Histogram
	BinaryLenDistribution *hdrhistogram.Histogram
}

type AttributesStats struct {
	TotalCount   int64
	Missing      int64
	Distribution *hdrhistogram.Histogram

	// Attribute type distribution
	I64TypeDistribution    *hdrhistogram.Histogram
	F64TypeDistribution    *hdrhistogram.Histogram
	BoolTypeDistribution   *hdrhistogram.Histogram
	StringTypeDistribution *hdrhistogram.Histogram
	BinaryTypeDistribution *hdrhistogram.Histogram
	ListTypeDistribution   *hdrhistogram.Histogram
	MapTypeDistribution    *hdrhistogram.Histogram

	// Attribute key
	KeyLenDistribution *hdrhistogram.Histogram
	KeyDistinctValue   *hyperloglog.Sketch

	// Attribute distinct values per type
	I64DistinctValue    *hyperloglog.Sketch
	F64DistinctValue    *hyperloglog.Sketch
	StringDistinctValue *hyperloglog.Sketch
	BinaryDistinctValue *hyperloglog.Sketch

	// Content length distribution
	StringLenDistribution *hdrhistogram.Histogram
	BinaryLenDistribution *hdrhistogram.Histogram

	// Dropped Attributes Count distribution
	DACDistinctValue *hyperloglog.Sketch
}

type StringStats struct {
	Missing         int64
	LenDistribution *hdrhistogram.Histogram
	DistinctValue   *hyperloglog.Sketch
}

func NewAnyValueStats() *AnyValueStats {
	return &AnyValueStats{
		I64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		F64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		BoolTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		StringTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		ListTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		MapTypeDistribution:    hdrhistogram.New(1, 1000000, 2),

		I64DistinctValue:    hyperloglog.New16(),
		F64DistinctValue:    hyperloglog.New16(),
		StringDistinctValue: hyperloglog.New16(),
		BinaryDistinctValue: hyperloglog.New16(),

		StringLenDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryLenDistribution: hdrhistogram.New(1, 1000000, 2),
	}
}

func NewAttributesStats() *AttributesStats {
	return &AttributesStats{
		Distribution: hdrhistogram.New(1, 1000000, 2),

		I64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		F64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		BoolTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		StringTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		ListTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		MapTypeDistribution:    hdrhistogram.New(1, 1000000, 2),

		KeyLenDistribution: hdrhistogram.New(0, 1000, 2),
		KeyDistinctValue:   hyperloglog.New16(),

		I64DistinctValue:    hyperloglog.New16(),
		F64DistinctValue:    hyperloglog.New16(),
		StringDistinctValue: hyperloglog.New16(),
		BinaryDistinctValue: hyperloglog.New16(),

		StringLenDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryLenDistribution: hdrhistogram.New(1, 1000000, 2),

		DACDistinctValue: hyperloglog.New16(),
	}
}

func (a *AttributesStats) UpdateWith(attrs pcommon.Map, dac uint32) {
	if attrs.Len() == 0 {
		a.Missing++
		return
	}

	a.TotalCount++

	RequireNoError(a.Distribution.RecordValue(int64(attrs.Len())))

	var (
		i64Count    int64
		f64Count    int64
		boolCount   int64
		stringCount int64
		binaryCount int64
		listCount   int64
		mapCount    int64
	)

	attrs.Range(func(k string, v pcommon.Value) bool {
		RequireNoError(a.KeyLenDistribution.RecordValue(int64(len(k))))
		a.KeyDistinctValue.Insert([]byte(k))

		switch v.Type() {
		case pcommon.ValueTypeInt:
			i64Count++
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v.Int()))
			a.I64DistinctValue.Insert(b)
		case pcommon.ValueTypeDouble:
			f64Count++
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, math.Float64bits(v.Double()))
			a.F64DistinctValue.Insert(b)
		case pcommon.ValueTypeBool:
			boolCount++
		case pcommon.ValueTypeStr:
			stringCount++
			a.StringDistinctValue.Insert([]byte(v.Str()))
			RequireNoError(a.StringLenDistribution.RecordValue(int64(len(v.Str()))))
		case pcommon.ValueTypeBytes:
			binaryCount++
			a.BinaryDistinctValue.Insert(v.Bytes().AsRaw())
			RequireNoError(a.BinaryLenDistribution.RecordValue(int64(len(v.Bytes().AsRaw()))))
		case pcommon.ValueTypeSlice:
			listCount++
		case pcommon.ValueTypeMap:
			mapCount++
		default:
			// no-op
		}

		return true
	})

	if i64Count > 0 {
		RequireNoError(a.I64TypeDistribution.RecordValue(i64Count))
	}
	if f64Count > 0 {
		RequireNoError(a.F64TypeDistribution.RecordValue(f64Count))
	}
	if boolCount > 0 {
		RequireNoError(a.BoolTypeDistribution.RecordValue(boolCount))
	}
	if stringCount > 0 {
		RequireNoError(a.StringTypeDistribution.RecordValue(stringCount))
	}
	if binaryCount > 0 {
		RequireNoError(a.BinaryTypeDistribution.RecordValue(binaryCount))
	}
	if listCount > 0 {
		RequireNoError(a.ListTypeDistribution.RecordValue(listCount))
	}
	if mapCount > 0 {
		RequireNoError(a.MapTypeDistribution.RecordValue(mapCount))
	}

	if dac > 0 {
		a.DACDistinctValue.Insert([]byte(fmt.Sprintf("%d", dac)))
	}
}

func (a *AttributesStats) IsPresent() bool {
	return a.TotalCount > 0
}

func (a *AttributesStats) ShowStats(indent string, title string, color string) {
	if !a.IsPresent() {
		print(Grey)
		fmt.Printf("%sNo %s%s\n", indent, title, ColorReset)
		return
	}

	fmt.Printf("%s%s%s%s |Missing|    Min|    Max|   Mean|  Stdev|    P50|    P99|\n", indent, color, title, ColorReset)
	fmt.Printf("%s%s |%7d|%7d|%7d|%7.1f|%7.1f|%7d|%7d|\n", indent, strings.Repeat(" ", len(title)),
		a.Missing, a.Distribution.Min(), a.Distribution.Max(), a.Distribution.Mean(), a.Distribution.StdDev(), a.Distribution.ValueAtQuantile(50), a.Distribution.ValueAtQuantile(99),
	)
	indentChildren := indent + "  "

	print(Cyan)
	fmt.Printf("%sType%s     | Total|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indentChildren, ColorReset)
	if a.I64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sI64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.I64TypeDistribution.TotalCount(), a.I64TypeDistribution.Min(), a.I64TypeDistribution.Max(), a.I64TypeDistribution.Mean(), a.I64TypeDistribution.StdDev(), a.I64TypeDistribution.ValueAtQuantile(50), a.I64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.F64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sF64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.F64TypeDistribution.TotalCount(), a.F64TypeDistribution.Min(), a.F64TypeDistribution.Max(), a.F64TypeDistribution.Mean(), a.F64TypeDistribution.StdDev(), a.F64TypeDistribution.ValueAtQuantile(50), a.F64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BoolTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBool   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.BoolTypeDistribution.TotalCount(), a.BoolTypeDistribution.Min(), a.BoolTypeDistribution.Max(), a.BoolTypeDistribution.Mean(), a.BoolTypeDistribution.StdDev(), a.BoolTypeDistribution.ValueAtQuantile(50), a.BoolTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.StringTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sString |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.StringTypeDistribution.TotalCount(), a.StringTypeDistribution.Min(), a.StringTypeDistribution.Max(), a.StringTypeDistribution.Mean(), a.StringTypeDistribution.StdDev(), a.StringTypeDistribution.ValueAtQuantile(50), a.StringTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBinary |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.BinaryTypeDistribution.TotalCount(), a.BinaryTypeDistribution.Min(), a.BinaryTypeDistribution.Max(), a.BinaryTypeDistribution.Mean(), a.BinaryTypeDistribution.StdDev(), a.BinaryTypeDistribution.ValueAtQuantile(50), a.BinaryTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.ListTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sList   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.ListTypeDistribution.TotalCount(), a.ListTypeDistribution.Min(), a.ListTypeDistribution.Max(), a.ListTypeDistribution.Mean(), a.ListTypeDistribution.StdDev(), a.ListTypeDistribution.ValueAtQuantile(50), a.ListTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.MapTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sMap    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.MapTypeDistribution.TotalCount(), a.MapTypeDistribution.Min(), a.MapTypeDistribution.Max(), a.MapTypeDistribution.Mean(), a.MapTypeDistribution.StdDev(), a.MapTypeDistribution.ValueAtQuantile(50), a.MapTypeDistribution.ValueAtQuantile(99),
		)
	}

	print(Cyan)
	fmt.Printf("%sKey%s      |Distinct|Len Min|Len Max|Len Mean|Len Stdev|Len P50|Len P99|\n", indentChildren, ColorReset)
	fmt.Printf("%s         |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indentChildren,
		a.KeyDistinctValue.Estimate(), a.KeyLenDistribution.Min(), a.KeyLenDistribution.Max(), a.KeyLenDistribution.Mean(), a.KeyLenDistribution.StdDev(), a.KeyLenDistribution.ValueAtQuantile(50), a.KeyLenDistribution.ValueAtQuantile(99),
	)

	print(Cyan)
	fmt.Printf("%sValue%s    |Distinct|Len Min|Len Max|Len Mean|Len Stdev|Len P50|Len P99|\n", indentChildren, ColorReset)
	if a.I64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sI64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indentChildren+"  ", a.I64DistinctValue.Estimate())
	}
	if a.F64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sF64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indentChildren+"  ", a.F64DistinctValue.Estimate())
	}
	if a.StringDistinctValue.Estimate() > 0 {
		fmt.Printf("%sString |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indentChildren+"  ", a.StringDistinctValue.Estimate(),
			a.StringLenDistribution.Min(), a.StringLenDistribution.Max(), a.StringLenDistribution.Mean(), a.StringLenDistribution.StdDev(), a.StringLenDistribution.ValueAtQuantile(50), a.StringLenDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryDistinctValue.Estimate() > 0 {
		fmt.Printf("%sBinary |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indentChildren+"  ", a.BinaryDistinctValue.Estimate(),
			a.BinaryLenDistribution.Min(), a.BinaryLenDistribution.Max(), a.BinaryLenDistribution.Mean(), a.BinaryLenDistribution.StdDev(), a.BinaryLenDistribution.ValueAtQuantile(50), a.BinaryLenDistribution.ValueAtQuantile(99),
		)
	}

	if a.DACDistinctValue.Estimate() > 0 {
		print(Green)
		fmt.Printf("%sDroppedAttributesCount%s |Distinct|   Total|%%Distinct|\n", indent, ColorReset)
		fmt.Printf("%s                       |%8d|%8d|%8.1f%%|\n", indent,
			a.DACDistinctValue.Estimate(), a.TotalCount, float64(a.DACDistinctValue.Estimate())/float64(a.TotalCount)*100,
		)
	}
}

func (a *AnyValueStats) UpdateWith(value pcommon.Value) {
	if value.Type() == pcommon.ValueTypeEmpty {
		a.Missing++
		return
	}

	a.TotalCount++

	var (
		i64Count    int64
		f64Count    int64
		boolCount   int64
		stringCount int64
		binaryCount int64
		listCount   int64
		mapCount    int64
	)

	switch value.Type() {
	case pcommon.ValueTypeInt:
		i64Count++
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(value.Int()))
		a.I64DistinctValue.Insert(b)
	case pcommon.ValueTypeDouble:
		f64Count++
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(value.Double()))
		a.F64DistinctValue.Insert(b)
	case pcommon.ValueTypeBool:
		boolCount++
	case pcommon.ValueTypeStr:
		stringCount++
		a.StringDistinctValue.Insert([]byte(value.Str()))
		RequireNoError(a.StringLenDistribution.RecordValue(int64(len(value.Str()))))
	case pcommon.ValueTypeBytes:
		binaryCount++
		a.BinaryDistinctValue.Insert(value.Bytes().AsRaw())
		RequireNoError(a.BinaryLenDistribution.RecordValue(int64(len(value.Bytes().AsRaw()))))
	case pcommon.ValueTypeSlice:
		listCount++
	case pcommon.ValueTypeMap:
		mapCount++
	default:
		// no-op
	}

	if i64Count > 0 {
		RequireNoError(a.I64TypeDistribution.RecordValue(i64Count))
	}
	if f64Count > 0 {
		RequireNoError(a.F64TypeDistribution.RecordValue(f64Count))
	}
	if boolCount > 0 {
		RequireNoError(a.BoolTypeDistribution.RecordValue(boolCount))
	}
	if stringCount > 0 {
		RequireNoError(a.StringTypeDistribution.RecordValue(stringCount))
	}
	if binaryCount > 0 {
		RequireNoError(a.BinaryTypeDistribution.RecordValue(binaryCount))
	}
	if listCount > 0 {
		RequireNoError(a.ListTypeDistribution.RecordValue(listCount))
	}
	if mapCount > 0 {
		RequireNoError(a.MapTypeDistribution.RecordValue(mapCount))
	}
}

func (a *AnyValueStats) IsPresent() bool {
	return a.TotalCount > 0
}

func (a *AnyValueStats) ShowStats(indent string, title string, color string) {
	if !a.IsPresent() {
		print(Grey)
		fmt.Printf("%sNo %s%s\n", indent, title, ColorReset)
		return
	}

	fmt.Printf("%s%s%s%s |Missing|    Min|    Max|   Mean|  Stdev|    P50|    P99|\n", indent, color, title, ColorReset)
	fmt.Printf("%s%s |%7d|\n", indent, strings.Repeat(" ", len(title)),
		a.Missing,
	)
	indentChildren := indent + "  "

	print(Cyan)
	fmt.Printf("%sType%s     | Total|   Min|   Max|  Mean| Stdev|   P50|   P99|\n", indentChildren, ColorReset)
	if a.I64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sI64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.I64TypeDistribution.TotalCount(), a.I64TypeDistribution.Min(), a.I64TypeDistribution.Max(), a.I64TypeDistribution.Mean(), a.I64TypeDistribution.StdDev(), a.I64TypeDistribution.ValueAtQuantile(50), a.I64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.F64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sF64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.F64TypeDistribution.TotalCount(), a.F64TypeDistribution.Min(), a.F64TypeDistribution.Max(), a.F64TypeDistribution.Mean(), a.F64TypeDistribution.StdDev(), a.F64TypeDistribution.ValueAtQuantile(50), a.F64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BoolTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBool   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.BoolTypeDistribution.TotalCount(), a.BoolTypeDistribution.Min(), a.BoolTypeDistribution.Max(), a.BoolTypeDistribution.Mean(), a.BoolTypeDistribution.StdDev(), a.BoolTypeDistribution.ValueAtQuantile(50), a.BoolTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.StringTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sString |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.StringTypeDistribution.TotalCount(), a.StringTypeDistribution.Min(), a.StringTypeDistribution.Max(), a.StringTypeDistribution.Mean(), a.StringTypeDistribution.StdDev(), a.StringTypeDistribution.ValueAtQuantile(50), a.StringTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBinary |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.BinaryTypeDistribution.TotalCount(), a.BinaryTypeDistribution.Min(), a.BinaryTypeDistribution.Max(), a.BinaryTypeDistribution.Mean(), a.BinaryTypeDistribution.StdDev(), a.BinaryTypeDistribution.ValueAtQuantile(50), a.BinaryTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.ListTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sList   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.ListTypeDistribution.TotalCount(), a.ListTypeDistribution.Min(), a.ListTypeDistribution.Max(), a.ListTypeDistribution.Mean(), a.ListTypeDistribution.StdDev(), a.ListTypeDistribution.ValueAtQuantile(50), a.ListTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.MapTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sMap    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indentChildren+"  ",
			a.MapTypeDistribution.TotalCount(), a.MapTypeDistribution.Min(), a.MapTypeDistribution.Max(), a.MapTypeDistribution.Mean(), a.MapTypeDistribution.StdDev(), a.MapTypeDistribution.ValueAtQuantile(50), a.MapTypeDistribution.ValueAtQuantile(99),
		)
	}

	print(Cyan)
	fmt.Printf("%sValue%s    |Distinct|Len Min|Len Max|Len Mean|Len Stdev|Len P50|Len P99|\n", indentChildren, ColorReset)
	if a.I64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sI64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indentChildren+"  ", a.I64DistinctValue.Estimate())
	}
	if a.F64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sF64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indentChildren+"  ", a.F64DistinctValue.Estimate())
	}
	if a.StringDistinctValue.Estimate() > 0 {
		fmt.Printf("%sString |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indentChildren+"  ", a.StringDistinctValue.Estimate(),
			a.StringLenDistribution.Min(), a.StringLenDistribution.Max(), a.StringLenDistribution.Mean(), a.StringLenDistribution.StdDev(), a.StringLenDistribution.ValueAtQuantile(50), a.StringLenDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryDistinctValue.Estimate() > 0 {
		fmt.Printf("%sBinary |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indentChildren+"  ", a.BinaryDistinctValue.Estimate(),
			a.BinaryLenDistribution.Min(), a.BinaryLenDistribution.Max(), a.BinaryLenDistribution.Mean(), a.BinaryLenDistribution.StdDev(), a.BinaryLenDistribution.ValueAtQuantile(50), a.BinaryLenDistribution.ValueAtQuantile(99),
		)
	}
}

func NewStringStats() *StringStats {
	return &StringStats{
		LenDistribution: hdrhistogram.New(0, 1000000, 2),
		DistinctValue:   hyperloglog.New16(),
	}
}

func (s *StringStats) UpdateWith(str string) {
	if len(str) == 0 {
		s.Missing++
	}

	RequireNoError(s.LenDistribution.RecordValue(int64(len(str))))
	s.DistinctValue.Insert([]byte(str))
}

func (s *StringStats) ShowStats(name string, indent string) {
	if !s.IsPresent() {
		print(Grey)
		fmt.Printf("%sNo %s%s\n", indent, name, ColorReset)
		return
	}

	print(Green)
	fmt.Printf("%s%s%s |Missing|Distinct|Len Min|Len Max|Len Mean|Len Stdev|Len P50|Len P99|\n", indent, name, ColorReset)
	fmt.Printf("%s%s |%7d|%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indent, strings.Repeat(" ", len(name)),
		s.Missing, s.DistinctValue.Estimate(), s.LenDistribution.Min(), s.LenDistribution.Max(), s.LenDistribution.Mean(), s.LenDistribution.StdDev(), s.LenDistribution.ValueAtQuantile(50), s.LenDistribution.ValueAtQuantile(99))
}

func (s *StringStats) IsPresent() bool {
	return s.LenDistribution.TotalCount() > 0
}

func RequireNoError(err error) {
	if err != nil {
		panic(err)
	}
}

type TimeIntervalStats struct {
	TotalCount              int64
	StartTimeDistinctValue  *hyperloglog.Sketch
	EndTimeDistinctValue    *hyperloglog.Sketch
	IntervalDistinctValue   *hyperloglog.Sketch
	StartDeltaDistinctValue *hyperloglog.Sketch
	EndDeltaDistinctValue   *hyperloglog.Sketch
}

type TimestampStats struct {
	TotalCount        int64
	TimeDistinctValue *hyperloglog.Sketch
}

func NewTimeIntervalStats() *TimeIntervalStats {
	return &TimeIntervalStats{
		StartTimeDistinctValue:  hyperloglog.New16(),
		EndTimeDistinctValue:    hyperloglog.New16(),
		IntervalDistinctValue:   hyperloglog.New16(),
		StartDeltaDistinctValue: hyperloglog.New16(),
		EndDeltaDistinctValue:   hyperloglog.New16(),
	}
}

func (t *TimeIntervalStats) UpdateWithSpans(spans []ptrace.Span) {
	var prevStartTime time.Time
	var prevEndTime time.Time

	// Process StartDelta
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTimestamp().AsTime().Before(spans[j].StartTimestamp().AsTime())
	})
	for i, span := range spans {
		if i > 0 {
			startDelta := span.StartTimestamp().AsTime().Sub(prevStartTime)

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(startDelta.Nanoseconds()))
			t.StartDeltaDistinctValue.Insert(b)
		}

		prevStartTime = span.StartTimestamp().AsTime()
	}

	// Process EndDelta
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].EndTimestamp().AsTime().Before(spans[j].EndTimestamp().AsTime())
	})
	for i, span := range spans {
		if i > 0 {
			endDelta := span.EndTimestamp().AsTime().Sub(prevEndTime)

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(endDelta.Nanoseconds()))
			t.EndDeltaDistinctValue.Insert(b)
		}

		prevEndTime = span.EndTimestamp().AsTime()
	}

	for _, span := range spans {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(span.StartTimestamp()))
		t.StartTimeDistinctValue.Insert(b)
		binary.LittleEndian.PutUint64(b, uint64(span.EndTimestamp()))
		t.EndTimeDistinctValue.Insert(b)

		interval := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())
		binary.LittleEndian.PutUint64(b, uint64(interval.Nanoseconds()))
		t.IntervalDistinctValue.Insert(b)

	}

	t.TotalCount += int64(len(spans))
}

func (t *TimeIntervalStats) ShowStats(indent string) {
	print(Green)
	fmt.Printf("%sStartTimestamp%s (Distinct=%d)\n", indent, ColorReset, t.StartTimeDistinctValue.Estimate())
	print(Green)
	fmt.Printf("%sEndTimestamp%s (Distinct=%d)\n", indent, ColorReset, t.EndTimeDistinctValue.Estimate())

	indent += "  "
	print(Cyan)
	fmt.Printf("%sTime interval%s |Distinct|   Total|%%Distinct|\n", indent, ColorReset)
	indent += "  "
	fmt.Printf("%sEnd-Start   |%8d|%8d|%8.1f%%|\n", indent, t.IntervalDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.IntervalDistinctValue.Estimate())/float64(t.TotalCount))
	fmt.Printf("%sStart delta |%8d|%8d|%8.1f%%|\n", indent, t.StartDeltaDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.StartDeltaDistinctValue.Estimate())/float64(t.TotalCount))
	fmt.Printf("%sEnd delta   |%8d|%8d|%8.1f%%|\n", indent, t.EndDeltaDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.EndDeltaDistinctValue.Estimate())/float64(t.TotalCount))
}

func NewTimestampStats() *TimestampStats {
	return &TimestampStats{
		TimeDistinctValue: hyperloglog.New16(),
	}
}

func (t *TimestampStats) UpdateWith(timestamp pcommon.Timestamp) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(timestamp.AsTime().UnixNano()))
	t.TimeDistinctValue.Insert(b)
	t.TotalCount++
}

func (t *TimestampStats) ShowStats(title, indent string) {
	print(Green)
	fmt.Printf("%s%s%s |Distinct|   Total|%%Distinct|\n", indent, title, ColorReset)
	fmt.Printf("%s%s |%8d|%8d|%8.1f%%|\n", indent, strings.Repeat(" ", len(title)), t.TimeDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.TimeDistinctValue.Estimate())/float64(t.TotalCount))
}

type SchemaUrlStats struct {
	Missing          int64
	NonEmpty         int64
	SizeDistribution *hdrhistogram.Histogram
}

func (s *SchemaUrlStats) UpdateWith(schemaUrl string) {
	if schemaUrl == "" {
		s.Missing++
	} else {
		s.NonEmpty++
		RequireNoError(s.SizeDistribution.RecordValue(int64(len(schemaUrl))))
	}
}

func (s *SchemaUrlStats) ShowStats(indent string) {
	fmt.Printf("%sSchemaUrl string length distribution (total-count, missing, min, max, mean, stdev, p50, p99): %d, %d, %d, %d, %f, %f, %d, %d\n", indent,
		s.SizeDistribution.TotalCount(), s.Missing, s.SizeDistribution.Min(), s.SizeDistribution.Max(), s.SizeDistribution.Mean(), s.SizeDistribution.StdDev(), s.SizeDistribution.ValueAtQuantile(50), s.SizeDistribution.ValueAtQuantile(99),
	)
}

func NewStatusStats() *StatusStats {
	return &StatusStats{
		CodeDistinctValue:      hyperloglog.New16(),
		MessageDistincValue:    hyperloglog.New16(),
		MessageLenDistribution: hdrhistogram.New(0, 10000, 2),
	}
}
