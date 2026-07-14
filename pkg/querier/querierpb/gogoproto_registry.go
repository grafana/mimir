// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import (
	"github.com/gogo/protobuf/proto"
)

// The gogoproto-generated code registered every message with the gogo/protobuf
// registry from its init(). dispatcher.go calls proto.MessageName on
// querierpb types to identify inbound scheduler Any payloads; wiresmith does not
// register with the gogo registry, so we keep the gogo registrations here.
// The wiresmith-generated types satisfy gogo's proto.Message interface
// (Reset/String/ProtoMessage) and proto.Unmarshaler, so gogo can instantiate
// and decode them directly.
func init() {
	proto.RegisterType((*EvaluateQueryRequest)(nil), "querierpb.EvaluateQueryRequest")
	proto.RegisterType((*EvaluationNode)(nil), "querierpb.EvaluationNode")
	proto.RegisterType((*EvaluateQueryResponse)(nil), "querierpb.EvaluateQueryResponse")
	proto.RegisterType((*EvaluateQueryResponseSeriesMetadata)(nil), "querierpb.EvaluateQueryResponseSeriesMetadata")
	proto.RegisterType((*SeriesMetadata)(nil), "querierpb.SeriesMetadata")
	proto.RegisterType((*EvaluateQueryResponseStringValue)(nil), "querierpb.EvaluateQueryResponseStringValue")
	proto.RegisterType((*EvaluateQueryResponseScalarValue)(nil), "querierpb.EvaluateQueryResponseScalarValue")
	proto.RegisterType((*EvaluateQueryResponseInstantVectorSeriesData)(nil), "querierpb.EvaluateQueryResponseInstantVectorSeriesData")
	proto.RegisterType((*InstantVectorSeriesData)(nil), "querierpb.InstantVectorSeriesData")
	proto.RegisterType((*EvaluateQueryResponseRangeVectorStepData)(nil), "querierpb.EvaluateQueryResponseRangeVectorStepData")
	proto.RegisterType((*Error)(nil), "querierpb.Error")
	proto.RegisterType((*EvaluateQueryResponseEvaluationCompleted)(nil), "querierpb.EvaluateQueryResponseEvaluationCompleted")
	proto.RegisterType((*Annotations)(nil), "querierpb.Annotations")
}
