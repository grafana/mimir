// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"github.com/gogo/protobuf/proto"
)

// The gogoproto-generated code registered every message with the gogo/protobuf
// registry from its init(). results_cache.go stores query responses in the
// Extent.response google.protobuf.Any using gogo types.MarshalAny /
// types.EmptyAny / types.UnmarshalAny, which derive and resolve the Any type
// URL via proto.MessageName against the gogo registry. wiresmith does not
// register with the gogo registry, so we keep the gogo registrations here,
// under the same names the gogoproto output used, so type URLs stay
// wire-compatible with responses cached by older versions. The
// wiresmith-generated types satisfy gogo's proto.Message interface
// (Reset/String/ProtoMessage) and proto.Unmarshaler, so gogo can instantiate
// and decode them directly.
func init() {
	proto.RegisterType((*PrometheusHeader)(nil), "queryrange.PrometheusHeader")
	proto.RegisterType((*PrometheusResponse)(nil), "queryrange.PrometheusResponse")
	proto.RegisterType((*PrometheusData)(nil), "queryrange.PrometheusData")
	proto.RegisterType((*SampleStream)(nil), "queryrange.SampleStream")
	proto.RegisterType((*CachedError)(nil), "queryrange.CachedError")
	proto.RegisterType((*CachedResponse)(nil), "queryrange.CachedResponse")
	proto.RegisterType((*Extent)(nil), "queryrange.Extent")
	proto.RegisterType((*QueryStatistics)(nil), "queryrange.QueryStatistics")
	proto.RegisterType((*CachedHTTPResponse)(nil), "queryrange.CachedHTTPResponse")
	proto.RegisterType((*CachedHTTPHeader)(nil), "queryrange.CachedHTTPHeader")
}
