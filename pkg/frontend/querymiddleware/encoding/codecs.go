// SPDX-License-Identifier: AGPL-3.0-only

package encoding

// This is in the production package (rather than the test package) so that we can use it in
// tools/payload-size-stats.
var KnownCodecs = map[string]Codec{
	"original JSON":       OriginalJsonCodec{},
	"uninterned protobuf": UninternedProtobufCodec{},
	"interned protobuf":   InternedProtobufCodec{},
	"interned protobuf with single string symbol table": InternedProtobufWithSingleStringCodec{},
	"gzipped uninterned protobuf":                       GzipWrapperCodec{UninternedProtobufCodec{}},
	"snappy compressed uninterned protobuf":             SnappyWrapperCodec{UninternedProtobufCodec{}},
	"Arrow":                                             NewArrowCodec(),
}
