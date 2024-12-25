package csproto

// WireType defines the supported values for Protobuf wire encodings.
type WireType int

const (
	// WireTypeVarint denotes a value that is encoded using base128 varint encoding.
	WireTypeVarint WireType = 0
	// WireTypeFixed64 denotes a value that is encoded using 8 bytes.
	WireTypeFixed64 WireType = 1
	// WireTypeLengthDelimited denotes a value that is encoded as a sequence of bytes preceded by a
	// varint-encoded length.
	WireTypeLengthDelimited WireType = 2
	// WireTypeFixed32 denotes a value that is encoded using 4 bytes.
	WireTypeFixed32 WireType = 5

	// WireTypeStartGroup      WireType = 3
	// WireTypeEndGroup        WireType = 4
)

var (
	wireTypeToString = map[WireType]string{
		WireTypeVarint:          "varint",
		WireTypeFixed64:         "fixed64",
		WireTypeLengthDelimited: "length-delimited",
		WireTypeFixed32:         "fixed32",
	}
)

// String returns a string representation of wt.
func (wt WireType) String() string {
	if s, ok := wireTypeToString[wt]; ok {
		return s
	}
	return "unknown"
}
