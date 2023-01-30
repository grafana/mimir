// SPDX-License-Identifier: AGPL-3.0-only

package encoding

// This is in the production package (rather than the test package) so that we can use it in
// tools/payload-size-stats.
var KnownCodecs = map[string]Codec{
	"original JSON":                                                       OriginalJsonCodec{},
	"gzipped original JSON":                                               GzipWrapperCodec{OriginalJsonCodec{}},
	"snappy compressed original JSON":                                     SnappyWrapperCodec{OriginalJsonCodec{}},
	"original query middleware protobuf":                                  OriginalQueryMiddlewareProtobufCodec{},
	"snappy compressed original query middleware protobuf":                SnappyWrapperCodec{OriginalQueryMiddlewareProtobufCodec{}},
	"uninterned protobuf":                                                 UninternedProtobufCodec{},
	"interned protobuf":                                                   InternedProtobufCodec{},
	"snappy compressed interned protobuf":                                 SnappyWrapperCodec{InternedProtobufCodec{}},
	"interned protobuf with single string symbol table":                   InternedProtobufWithSingleStringCodec{},
	"packed interned protobuf":                                            PackedInternedProtobufCodec{},
	"packed interned protobuf with relative timestamps":                   PackedInternedProtobufWithRelativeTimestampsCodec{},
	"snappy compressed packed interned protobuf with relative timestamps": SnappyWrapperCodec{PackedInternedProtobufWithRelativeTimestampsCodec{}},
	"snappy compressed packed interned protobuf":                          SnappyWrapperCodec{PackedInternedProtobufCodec{}},
	"packed interned protobuf with single string symbol table":            PackedInternedProtobufWithSingleStringCodec{},
	"gzipped uninterned protobuf":                                         GzipWrapperCodec{UninternedProtobufCodec{}},
	"snappy compressed uninterned protobuf":                               SnappyWrapperCodec{UninternedProtobufCodec{}},
	"Arrow":                                                               NewArrowCodec(),
	"snappy compressed Arrow":                                             SnappyWrapperCodec{NewArrowCodec()},
}
