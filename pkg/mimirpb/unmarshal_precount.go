// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

// preCountWriteRequestFields does a fast scan of the protobuf wire format to count
// the number of timeseries (field 1 or 5) and metadata (field 3) entries.
// This allows pre-allocating slices to exact capacity, avoiding repeated append-driven
// geometric growth allocations during unmarshal.
func preCountWriteRequestFields(dAtA []byte) (timeseriesCount, metadataCount int) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		// Decode tag (varint)
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if iNdEx >= l {
				return
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)

		switch wireType {
		case 0: // varint
			for iNdEx < l {
				if dAtA[iNdEx] < 0x80 {
					iNdEx++
					break
				}
				iNdEx++
			}
		case 1: // 64-bit
			iNdEx += 8
		case 2: // length-delimited
			var length uint64
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			switch fieldNum {
			case 1: // Timeseries (RW1)
				timeseriesCount++
			case 3: // Metadata
				metadataCount++
			case 5: // TimeseriesRW2
				timeseriesCount++
			}
			iNdEx += int(length)
		case 5: // 32-bit
			iNdEx += 4
		default:
			return
		}
	}
	return
}

// preCountTimeSeriesFields does a fast scan of a TimeSeries protobuf wire format
// to count the number of labels (field 1), samples (field 2), exemplars (field 3),
// and histograms (field 4) entries. This allows pre-allocating slices to exact
// capacity before the actual unmarshal.
func preCountTimeSeriesFields(dAtA []byte) (labelCount, sampleCount, exemplarCount, histogramCount int) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if iNdEx >= l {
				return
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)

		switch wireType {
		case 0: // varint
			for iNdEx < l {
				if dAtA[iNdEx] < 0x80 {
					iNdEx++
					break
				}
				iNdEx++
			}
		case 1: // 64-bit
			iNdEx += 8
		case 2: // length-delimited
			var length uint64
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			switch fieldNum {
			case 1: // Labels
				labelCount++
			case 2: // Samples
				sampleCount++
			case 3: // Exemplars
				exemplarCount++
			case 4: // Histograms
				histogramCount++
			}
			iNdEx += int(length)
		case 5: // 32-bit
			iNdEx += 4
		default:
			return
		}
	}
	return
}
