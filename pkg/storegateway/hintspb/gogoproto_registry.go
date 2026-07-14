// SPDX-License-Identifier: AGPL-3.0-only

package hintspb

import (
	"github.com/gogo/protobuf/proto"
)

// The gogoproto-generated code registered every message with the gogo/protobuf
// registry from its init(). hintspb messages are the payloads of the deprecated
// storepb "hints" google.protobuf.Any fields: the querier and store-gateway
// pack/unpack them with gogo types.MarshalAny / types.UnmarshalAny, which derive
// and match the Any type URL via proto.MessageName against the gogo registry.
// wiresmith does not register with the gogo registry, so we keep the gogo
// registrations here, under the same names the gogoproto output used, so type
// URLs stay wire-compatible with older peers. The wiresmith-generated types
// satisfy gogo's proto.Message interface (Reset/String/ProtoMessage) and
// proto.Unmarshaler, so gogo can instantiate and decode them directly.
func init() {
	proto.RegisterType((*SeriesRequestHints)(nil), "hintspb.SeriesRequestHints")
	proto.RegisterType((*SeriesResponseHints)(nil), "hintspb.SeriesResponseHints")
	proto.RegisterType((*Block)(nil), "hintspb.Block")
	proto.RegisterType((*LabelNamesRequestHints)(nil), "hintspb.LabelNamesRequestHints")
	proto.RegisterType((*LabelNamesResponseHints)(nil), "hintspb.LabelNamesResponseHints")
	proto.RegisterType((*LabelValuesRequestHints)(nil), "hintspb.LabelValuesRequestHints")
	proto.RegisterType((*LabelValuesResponseHints)(nil), "hintspb.LabelValuesResponseHints")
}
