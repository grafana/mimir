// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"github.com/gogo/protobuf/proto"
)

// The gogoproto-generated code registered every message and enum with the
// gogo/protobuf registry from its init(). Some dependencies still resolve
// messages through that registry — most importantly github.com/gogo/status
// (used via dskit's grpcutil), which unmarshals google.protobuf.Any error
// details by looking up the type name. wiresmith only registers with
// google.golang.org/protobuf's registry, so we keep the gogo registrations
// here. The wiresmith-generated types satisfy gogo's proto.Message interface
// (Reset/String/ProtoMessage) and proto.Unmarshaler, so gogo can instantiate
// and decode them directly.
func init() {
	proto.RegisterEnum("cortexpb.ErrorCause", ErrorCause_name, ErrorCause_value)
	proto.RegisterEnum("cortexpb.QueryStatus", QueryStatus_name, QueryStatus_value)
	proto.RegisterEnum("cortexpb.QueryErrorType", QueryErrorType_name, QueryErrorType_value)
	proto.RegisterEnum("cortexpb.WriteRequest_SourceEnum", WriteRequest_SourceEnum_name, WriteRequest_SourceEnum_value)
	proto.RegisterEnum("cortexpb.MetricMetadata_MetricType", MetricMetadata_MetricType_name, MetricMetadata_MetricType_value)
	proto.RegisterEnum("cortexpb.Histogram_ResetHint", Histogram_ResetHint_name, Histogram_ResetHint_value)
	proto.RegisterEnum("cortexpb.MetadataRW2_MetricType", MetadataRW2_MetricType_name, MetadataRW2_MetricType_value)
	proto.RegisterType((*WriteRequest)(nil), "cortexpb.WriteRequest")
	proto.RegisterType((*WriteResponse)(nil), "cortexpb.WriteResponse")
	proto.RegisterType((*ErrorDetails)(nil), "cortexpb.ErrorDetails")
	proto.RegisterType((*TimeSeries)(nil), "cortexpb.TimeSeries")
	proto.RegisterType((*LabelPair)(nil), "cortexpb.LabelPair")
	proto.RegisterType((*Sample)(nil), "cortexpb.Sample")
	proto.RegisterType((*MetricMetadata)(nil), "cortexpb.MetricMetadata")
	proto.RegisterType((*Metric)(nil), "cortexpb.Metric")
	proto.RegisterType((*Exemplar)(nil), "cortexpb.Exemplar")
	proto.RegisterType((*Histogram)(nil), "cortexpb.Histogram")
	proto.RegisterType((*FloatHistogram)(nil), "cortexpb.FloatHistogram")
	proto.RegisterType((*BucketSpan)(nil), "cortexpb.BucketSpan")
	proto.RegisterType((*FloatHistogramPair)(nil), "cortexpb.FloatHistogramPair")
	proto.RegisterType((*SampleHistogram)(nil), "cortexpb.SampleHistogram")
	proto.RegisterType((*HistogramBucket)(nil), "cortexpb.HistogramBucket")
	proto.RegisterType((*SampleHistogramPair)(nil), "cortexpb.SampleHistogramPair")
	proto.RegisterType((*QueryResponse)(nil), "cortexpb.QueryResponse")
	proto.RegisterType((*StringData)(nil), "cortexpb.StringData")
	proto.RegisterType((*VectorData)(nil), "cortexpb.VectorData")
	proto.RegisterType((*VectorSample)(nil), "cortexpb.VectorSample")
	proto.RegisterType((*VectorHistogram)(nil), "cortexpb.VectorHistogram")
	proto.RegisterType((*ScalarData)(nil), "cortexpb.ScalarData")
	proto.RegisterType((*MatrixData)(nil), "cortexpb.MatrixData")
	proto.RegisterType((*MatrixSeries)(nil), "cortexpb.MatrixSeries")
	proto.RegisterType((*WriteRequestRW2)(nil), "cortexpb.WriteRequestRW2")
	proto.RegisterType((*TimeSeriesRW2)(nil), "cortexpb.TimeSeriesRW2")
	proto.RegisterType((*ExemplarRW2)(nil), "cortexpb.ExemplarRW2")
	proto.RegisterType((*MetadataRW2)(nil), "cortexpb.MetadataRW2")
}
