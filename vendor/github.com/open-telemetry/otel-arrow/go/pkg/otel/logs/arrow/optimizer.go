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
	"bytes"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
)

type (
	LogsOptimizer struct {
		sorter LogSorter
	}

	LogsOptimized struct {
		Logs []*FlattenedLog
	}

	ResScope struct {
		// Resource log section.
		ResourceLogsID    int
		Resource          pcommon.Resource
		ResourceSchemaUrl string

		// Scope log section.
		ScopeLogsID    int
		Scope          pcommon.InstrumentationScope
		ScopeSchemaUrl string
	}

	FlattenedLog struct {
		ResScope *ResScope
		Log      plog.LogRecord
	}

	LogSorter interface {
		Sort(logs []*FlattenedLog)
	}

	LogsByNothing                          struct{}
	LogsByResourceLogsIDScopeLogsIDTraceID struct{}
)

func NewLogsOptimizer(sorter LogSorter) *LogsOptimizer {
	return &LogsOptimizer{
		sorter: sorter,
	}
}

func (t *LogsOptimizer) Optimize(logs plog.Logs) *LogsOptimized {
	logsOptimized := &LogsOptimized{
		Logs: make([]*FlattenedLog, 0, 32),
	}

	resLogsIDs := make(map[string]int)
	scopeLogsIDs := make(map[string]int)

	resLogsSlice := logs.ResourceLogs()
	for i := 0; i < resLogsSlice.Len(); i++ {
		resLogs := resLogsSlice.At(i)
		resource := resLogs.Resource()
		resourceSchemaUrl := resLogs.SchemaUrl()
		ID := otlp.ResourceID(resource, resourceSchemaUrl)
		resLogsID, found := resLogsIDs[ID]
		if !found {
			resLogsID = len(resLogsIDs)
			resLogsIDs[ID] = resLogsID
		}

		scopeLogs := resLogs.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeSpan := scopeLogs.At(j)
			scope := scopeSpan.Scope()
			scopeSchemaUrl := scopeSpan.SchemaUrl()
			ID = otlp.ScopeID(scope, scopeSchemaUrl)
			scopeLogsID, found := scopeLogsIDs[ID]
			if !found {
				scopeLogsID = len(scopeLogsIDs)
				scopeLogsIDs[ID] = scopeLogsID
			}

			resScope := &ResScope{
				ResourceLogsID:    resLogsID,
				Resource:          resource,
				ResourceSchemaUrl: resourceSchemaUrl,
				ScopeLogsID:       scopeLogsID,
				Scope:             scope,
				ScopeSchemaUrl:    scopeSchemaUrl,
			}

			logRecords := scopeSpan.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logsOptimized.Logs = append(logsOptimized.Logs, &FlattenedLog{
					ResScope: resScope,
					Log:      logRecords.At(k),
				})
			}
		}
	}

	t.sorter.Sort(logsOptimized.Logs)

	return logsOptimized
}

// No sorting
// ==========

func UnsortedLogs() *LogsByNothing {
	return &LogsByNothing{}
}

func (s *LogsByNothing) Sort(_ []*FlattenedLog) {
}

// Sort logs by resource logs ID, scope logs ID, and trace ID.
func SortLogsByResourceLogsIDScopeLogsIDTraceID() *LogsByResourceLogsIDScopeLogsIDTraceID {
	return &LogsByResourceLogsIDScopeLogsIDTraceID{}
}

func (s *LogsByResourceLogsIDScopeLogsIDTraceID) Sort(logs []*FlattenedLog) {
	sort.Slice(logs, func(i, j int) bool {
		logI := logs[i]
		logJ := logs[j]

		if logI.ResScope.ResourceLogsID == logJ.ResScope.ResourceLogsID {
			if logI.ResScope.ScopeLogsID == logJ.ResScope.ScopeLogsID {
				traceIdI := logI.Log.TraceID()
				traceIdJ := logJ.Log.TraceID()
				return bytes.Compare(traceIdI[:], traceIdJ[:]) == -1
			} else {
				return logI.ResScope.ScopeLogsID < logJ.ResScope.ScopeLogsID
			}
		} else {
			return logI.ResScope.ResourceLogsID < logJ.ResScope.ResourceLogsID
		}
	})
}
