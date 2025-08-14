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

package constants

// All constants used a column names.

const ResourceMetrics = "resource_metrics"
const TimeUnixNano string = "time_unix_nano"
const StartTimeUnixNano string = "start_time_unix_nano"
const DurationTimeUnixNano string = "duration_time_unix_nano"
const ObservedTimeUnixNano string = "observed_time_unix_nano"
const SeverityNumber string = "severity_number"
const SeverityText string = "severity_text"
const DroppedAttributesCount string = "dropped_attributes_count"
const DroppedEventsCount string = "dropped_events_count"
const DroppedLinksCount string = "dropped_links_count"
const Flags string = "flags"
const TraceId string = "trace_id"
const TraceState string = "trace_state"
const SpanId string = "span_id"
const ParentSpanId string = "parent_span_id"
const Attributes string = "attributes"
const Resource string = "resource"
const ScopeMetrics string = "scope_metrics"
const Scope string = "scope"
const Name string = "name"
const KIND string = "kind"
const Version string = "version"
const Body string = "body"
const Status string = "status"
const Description string = "description"
const Unit string = "unit"
const Data string = "data"
const StatusMessage string = "status_message"
const StatusCode string = "code"
const SummaryCount string = "count"
const SummarySum string = "sum"
const SummaryQuantileValues string = "quantile"
const SummaryQuantile string = "quantile"
const SummaryValue string = "value"
const MetricValue string = "value"
const IntValue string = "int_value"
const DoubleValue string = "double_value"
const HistogramCount string = "count"
const HistogramSum string = "sum"
const HistogramMin string = "min"
const HistogramMax string = "max"
const HistogramBucketCounts string = "bucket_counts"
const HistogramExplicitBounds string = "explicit_bounds"
const ExpHistogramScale string = "scale"
const ExpHistogramZeroCount string = "zero_count"
const ExpHistogramPositive string = "positive"
const ExpHistogramNegative string = "negative"
const ExpHistogramOffset string = "offset"
const ExpHistogramBucketCounts string = "bucket_counts"
const SchemaUrl string = "schema_url"
const I64MetricValue string = "i64"
const F64MetricValue string = "f64"
const Exemplars string = "exemplars"
const IsMonotonic string = "is_monotonic"
const AggregationTemporality string = "aggregation_temporality"

const SharedAttributes string = "shared_attributes"
const SharedEventAttributes string = "shared_event_attributes"
const SharedLinkAttributes string = "shared_link_attributes"

const ID string = "id"
const ParentID string = "parent_id"

const MetricType string = "metric_type"

// Attributes

const AttributeKey string = "key"
const AttributeType string = "type"
const AttributeStr string = "str"
const AttributeInt string = "int"
const AttributeDouble string = "double"
const AttributeBool string = "bool"
const AttributeBytes string = "bytes"
const AttributeSer string = "ser"

// Log body

const BodyType string = "type"
const BodyStr string = "str"
const BodyInt string = "int"
const BodyDouble string = "double"
const BodyBool string = "bool"
const BodyBytes string = "bytes"
const BodySer string = "ser"

const SortingColumns string = "sorting_columns"
const Value string = "value"
