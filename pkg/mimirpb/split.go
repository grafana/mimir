package mimirpb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	wireTypeVarint = 0
	wireTypeLen    = 2

	timeseriesField              = 1
	sourceField                  = 2
	metadataField                = 3
	skipLabelNameValidationField = 1000

	boolValueVarintLength = 1 // bool value always encoded as 1 byte
)

var (
	sourceFieldTag     = tag(sourceField, wireTypeVarint)
	sourceFieldTagSize = varintLength(sourceFieldTag)

	skipLabelNameValidationFieldTag     = tag(skipLabelNameValidationField, wireTypeVarint)
	skipLabelNameValidationFieldTagSize = varintLength(skipLabelNameValidationFieldTag)

	maxExtraBytes = sourceFieldTagSize + varintLength(math.MaxInt32) + skipLabelNameValidationFieldTagSize + boolValueVarintLength
)

// SplitWriteRequestRequest splits marshalled WriteRequest, produced by WriteRequest.Marshal(),
// into subrequests with given max size.
//
// This function partially parses WriteRequest and splits the request at field boundaries.
// Some fields (source, skipLabelNameValidation) are copied into each returned subrequests.
//
// If this function encounters unknown field, it returns error.
func SplitWriteRequestRequest(writeRequest []byte, sizeLimit int) ([][]byte, error) {
	if len(writeRequest) <= sizeLimit {
		return [][]byte{writeRequest}, nil
	}

	// Save some bytes for extra source and skipLabelNameValidation fields
	// that we may need to add to all subrequests.
	sizeLimit -= maxExtraBytes

	// we always return at least 1 subrequest
	subrequests := make([][]byte, 1, 1+(len(writeRequest)/sizeLimit))
	// We start with empty request, and will keep extending it while we can.
	subrequests[0] = writeRequest[:0]

	// Remember last value of source and skipLabelNameValidation fields.
	hasSource := false
	source := int32(0)
	sourceSize := 0

	hasSkipLabelNameValidation := false
	skipLabelNameValidation := false

	// Iterate over field, and for each field decide whether we keep it in the current request (last one in subrequests slice),
	// or we create new subrequest. Values for fields like source and skipLabelNameValidation are remembered, and later copied
	// to all subrequests.
	//
	// It's useful to reference https://protobuf.dev/programming-guides/encoding/#cheat-sheet while reading this code.
	//
	// invariant: writeRequest starts at the next field.
	for len(writeRequest) > 0 {
		// Decode tag. Tag is (field numer << 3 | type), encoded as uint32 varint.
		tagVal, tagSize := binary.Uvarint(writeRequest)
		if tagSize <= 0 {
			return nil, io.ErrUnexpectedEOF
		}
		if tagVal > math.MaxUint32 {
			return nil, fmt.Errorf("invalid tag: %d", tagVal)
		}

		// decode tag into field number and type
		fieldNum := tagVal >> 3
		wireType := tagVal & 7

		// totalFieldSize is updated later depending on the type and covers entire field, starting with tag.
		totalFieldSize := tagSize

		switch fieldNum {
		case timeseriesField, metadataField:
			if wireType != wireTypeLen {
				return nil, fmt.Errorf("unexpected wire type for field %d: %d", fieldNum, wireType)
			}

			decodedLength, decodedLengthSize := binary.Uvarint(writeRequest[tagSize:])
			if decodedLengthSize <= 0 {
				return nil, io.ErrUnexpectedEOF
			}
			if decodedLength <= 0 || decodedLength > math.MaxInt32 {
				return nil, fmt.Errorf("invalid decoded length: %d", decodedLength)
			}
			if len(writeRequest) < int(decodedLength) {
				return nil, fmt.Errorf("length too big: %d", decodedLength)
			}

			totalFieldSize += decodedLengthSize + int(decodedLength)

		case sourceField, skipLabelNameValidationField:
			if wireType != wireTypeVarint {
				return nil, fmt.Errorf("unexpected wire type for field %d: %d", fieldNum, wireType)
			}

			val, valSize := binary.Uvarint(writeRequest[tagSize:])
			if valSize <= 0 {
				return nil, io.ErrUnexpectedEOF
			}

			if fieldNum == sourceField { // Source, any int32 value is allowed.
				if val > math.MaxInt32 {
					return nil, fmt.Errorf("invalid value %d for field %d", val, fieldNum)
				}
				hasSource = true
				source = int32(val)
				sourceSize = valSize
			}

			if fieldNum == skipLabelNameValidationField {
				hasSkipLabelNameValidation = true
				if val == 0 {
					skipLabelNameValidation = false
				} else {
					skipLabelNameValidation = true
				}
			}

			totalFieldSize += valSize

		default:
			// We can't handle unexpected fields.
			return nil, fmt.Errorf("unexpected field %d, type %d", fieldNum, wireType)
		}

		// index to current subrequest, always the last one in the slice.
		currIx := len(subrequests) - 1
		currSize := len(subrequests[currIx])
		// We want to extend current subrequest with this field, if possible. If not, start new subrequest with this field only.
		if currSize+totalFieldSize <= sizeLimit {
			subrequests[currIx] = subrequests[currIx][:currSize+totalFieldSize]
		} else {
			subrequests = append(subrequests, writeRequest[:totalFieldSize])
		}

		writeRequest = writeRequest[totalFieldSize:]
	}

	// We have all subrequests, now we need to post-process them in case there was source or skipLabelNameValidation field.
	// If there were, we add them to ALL subrequests.
	//
	// We may end up with subrequests with multiple source or skipLabelNameValidation fields,
	// but that's totally fine -- parsers are expected to honor last field value.

	extraBytes := 0
	if hasSource {
		extraBytes += sourceFieldTagSize + sourceSize
	}
	if hasSkipLabelNameValidation {
		extraBytes += skipLabelNameValidationFieldTagSize + boolValueVarintLength
	}

	if extraBytes > 0 {
		for ix := range subrequests {
			// Clone subrequest before appending bytes to it. Here we could use a buffer pool.
			cp := make([]byte, len(subrequests[ix])+extraBytes)
			copy(cp, subrequests[ix])

			appendIx := len(subrequests[ix])
			if hasSource {
				appendIx += putUvarintWithExpectedLength(cp[appendIx:], sourceFieldTag, sourceFieldTagSize)
				appendIx += putUvarintWithExpectedLength(cp[appendIx:], uint64(source), sourceSize)
			}

			if hasSkipLabelNameValidation {
				appendIx += putUvarintWithExpectedLength(cp[appendIx:], skipLabelNameValidationFieldTag, skipLabelNameValidationFieldTagSize)
				if skipLabelNameValidation {
					appendIx += putUvarintWithExpectedLength(cp[appendIx:], 1, boolValueVarintLength)
				} else {
					appendIx += putUvarintWithExpectedLength(cp[appendIx:], 0, boolValueVarintLength)
				}
			}

			subrequests[ix] = cp
		}
	}

	return subrequests, nil
}

func tag(field, typ uint64) uint64 {
	return field<<3 | typ
}

func varintLength(val uint64) int {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], val)
	return n
}

func putUvarintWithExpectedLength(buf []byte, val uint64, expLength int) int {
	n := binary.PutUvarint(buf, val)
	if n != expLength {
		panic(fmt.Sprintf("expected to write %d bytes, got %d", expLength, n))
	}
	return n
}
