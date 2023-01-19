// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import (
	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectorlog "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/logs/v1"
	otlplogs "go.opentelemetry.io/collector/pdata/internal/data/protogen/logs/v1"
)

// Logs is the top-level struct that is propagated through the logs pipeline.
// Use NewLogs to create new instance, zero-initialized instance is not valid for use.
type Logs internal.Logs

func newLogs(orig *otlpcollectorlog.ExportLogsServiceRequest) Logs {
	return Logs(internal.NewLogs(orig))
}

func (ms Logs) getOrig() *otlpcollectorlog.ExportLogsServiceRequest {
	return internal.GetOrigLogs(internal.Logs(ms))
}

// NewLogs creates a new Logs struct.
func NewLogs() Logs {
	return newLogs(&otlpcollectorlog.ExportLogsServiceRequest{})
}

// MoveTo moves the Logs instance overriding the destination and
// resetting the current instance to its zero value.
func (ms Logs) MoveTo(dest Logs) {
	*dest.getOrig() = *ms.getOrig()
	*ms.getOrig() = otlpcollectorlog.ExportLogsServiceRequest{}
}

// CopyTo copies the Logs instance overriding the destination.
func (ms Logs) CopyTo(dest Logs) {
	ms.ResourceLogs().CopyTo(dest.ResourceLogs())
}

// LogRecordCount calculates the total number of log records.
func (ms Logs) LogRecordCount() int {
	logCount := 0
	rss := ms.ResourceLogs()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ill := rs.ScopeLogs()
		for i := 0; i < ill.Len(); i++ {
			logs := ill.At(i)
			logCount += logs.LogRecords().Len()
		}
	}
	return logCount
}

// ResourceLogs returns the ResourceLogsSlice associated with this Logs.
func (ms Logs) ResourceLogs() ResourceLogsSlice {
	return newResourceLogsSlice(&ms.getOrig().ResourceLogs)
}

// SeverityNumber represents severity number of a log record.
type SeverityNumber int32

const (
	SeverityNumberUnspecified = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED)
	SeverityNumberTrace       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_TRACE)
	SeverityNumberTrace2      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_TRACE2)
	SeverityNumberTrace3      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_TRACE3)
	SeverityNumberTrace4      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_TRACE4)
	SeverityNumberDebug       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_DEBUG)
	SeverityNumberDebug2      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_DEBUG2)
	SeverityNumberDebug3      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_DEBUG3)
	SeverityNumberDebug4      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_DEBUG4)
	SeverityNumberInfo        = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO)
	SeverityNumberInfo2       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO2)
	SeverityNumberInfo3       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO3)
	SeverityNumberInfo4       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO4)
	SeverityNumberWarn        = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_WARN)
	SeverityNumberWarn2       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_WARN2)
	SeverityNumberWarn3       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_WARN3)
	SeverityNumberWarn4       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_WARN4)
	SeverityNumberError       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR)
	SeverityNumberError2      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR2)
	SeverityNumberError3      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR3)
	SeverityNumberError4      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR4)
	SeverityNumberFatal       = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_FATAL)
	SeverityNumberFatal2      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_FATAL2)
	SeverityNumberFatal3      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_FATAL3)
	SeverityNumberFatal4      = SeverityNumber(otlplogs.SeverityNumber_SEVERITY_NUMBER_FATAL4)
)

// String returns the string representation of the SeverityNumber.
func (sn SeverityNumber) String() string {
	switch sn {
	case SeverityNumberUnspecified:
		return "Unspecified"
	case SeverityNumberTrace:
		return "Trace"
	case SeverityNumberTrace2:
		return "Trace2"
	case SeverityNumberTrace3:
		return "Trace3"
	case SeverityNumberTrace4:
		return "Trace4"
	case SeverityNumberDebug:
		return "Debug"
	case SeverityNumberDebug2:
		return "Debug2"
	case SeverityNumberDebug3:
		return "Debug3"
	case SeverityNumberDebug4:
		return "Debug4"
	case SeverityNumberInfo:
		return "Info"
	case SeverityNumberInfo2:
		return "Info2"
	case SeverityNumberInfo3:
		return "Info3"
	case SeverityNumberInfo4:
		return "Info4"
	case SeverityNumberWarn:
		return "Warn"
	case SeverityNumberWarn2:
		return "Warn2"
	case SeverityNumberWarn3:
		return "Warn3"
	case SeverityNumberWarn4:
		return "Warn4"
	case SeverityNumberError:
		return "Error"
	case SeverityNumberError2:
		return "Error2"
	case SeverityNumberError3:
		return "Error3"
	case SeverityNumberError4:
		return "Error4"
	case SeverityNumberFatal:
		return "Fatal"
	case SeverityNumberFatal2:
		return "Fatal2"
	case SeverityNumberFatal3:
		return "Fatal3"
	case SeverityNumberFatal4:
		return "Fatal4"
	}
	return ""
}
