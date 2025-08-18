package encoding

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"reflect"

	"github.com/xitongsys/parquet-go/parquet"
)

func ToInt64(nums []interface{}) []int64 { //convert bool/int values to int64 values
	ln := len(nums)
	res := make([]int64, ln)
	if ln <= 0 {
		return res
	}
	tk := reflect.TypeOf(nums[0]).Kind()
	for i := 0; i < ln; i++ {
		if tk == reflect.Bool {
			if nums[i].(bool) {
				res[i] = 1
			} else {
				res[i] = 0
			}
		} else {
			res[i] = int64(reflect.ValueOf(nums[i]).Int())
		}
	}
	return res
}

func WritePlain(src []interface{}, pt parquet.Type) []byte {
	ln := len(src)
	if ln <= 0 {
		return []byte{}
	}

	if pt == parquet.Type_BOOLEAN {
		return WritePlainBOOLEAN(src)
	} else if pt == parquet.Type_INT32 {
		return WritePlainINT32(src)
	} else if pt == parquet.Type_INT64 {
		return WritePlainINT64(src)
	} else if pt == parquet.Type_INT96 {
		return WritePlainINT96(src)
	} else if pt == parquet.Type_FLOAT {
		return WritePlainFLOAT(src)
	} else if pt == parquet.Type_DOUBLE {
		return WritePlainDOUBLE(src)
	} else if pt == parquet.Type_BYTE_ARRAY {
		return WritePlainBYTE_ARRAY(src)
	} else if pt == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return WritePlainFIXED_LEN_BYTE_ARRAY(src)
	} else {
		return []byte{}
	}
}

func WritePlainBOOLEAN(nums []interface{}) []byte {
	ln := len(nums)
	byteNum := (ln + 7) / 8
	res := make([]byte, byteNum)
	for i := 0; i < ln; i++ {
		if nums[i].(bool) {
			res[i/8] = res[i/8] | (1 << uint32(i%8))
		}
	}
	return res
}

func WritePlainINT32(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	BinaryWriteINT32(bufWriter, nums)
	return bufWriter.Bytes()
}

func WritePlainINT64(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	BinaryWriteINT64(bufWriter, nums)
	return bufWriter.Bytes()
}

func WritePlainINT96(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	for i := 0; i < len(nums); i++ {
		bufWriter.WriteString(nums[i].(string))
	}
	return bufWriter.Bytes()
}

func WritePlainFLOAT(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	BinaryWriteFLOAT32(bufWriter, nums)
	return bufWriter.Bytes()
}

func WritePlainDOUBLE(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	BinaryWriteFLOAT64(bufWriter, nums)
	return bufWriter.Bytes()
}

func WritePlainBYTE_ARRAY(arrays []interface{}) []byte {
	bufLen := 0
	for i := 0; i < len(arrays); i++ {
		bufLen += 4 + len(arrays[i].(string))
	}

	buf := make([]byte, bufLen)
	pos := 0
	for i := 0; i < len(arrays); i++ {
		value := arrays[i].(string)
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
		pos += 4
		copy(buf[pos:pos+len(value)], value)
		pos += len(value)
	}
	return buf
}

func WritePlainFIXED_LEN_BYTE_ARRAY(arrays []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		bufWriter.WriteString(arrays[i].(string))
	}
	return bufWriter.Bytes()
}

func WriteUnsignedVarInt(num uint64) []byte {
	byteNum := (bits.Len64(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(numTmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

func WriteRLE(vals []interface{}, bitWidth int32, pt parquet.Type) []byte {
	ln := len(vals)
	i := 0
	res := make([]byte, 0)
	for i < ln {
		j := i + 1
		for j < ln && vals[j] == vals[i] {
			j++
		}
		num := j - i
		header := num << 1
		byteNum := (bitWidth + 7) / 8
		headerBuf := WriteUnsignedVarInt(uint64(header))

		valBuf := WritePlain([]interface{}{vals[i]}, pt)

		rleBuf := make([]byte, int64(len(headerBuf))+int64(byteNum))
		copy(rleBuf[0:], headerBuf)
		copy(rleBuf[len(headerBuf):], valBuf[0:byteNum])
		res = append(res, rleBuf...)
		i = j
	}
	return res
}

func WriteRLEBitPackedHybrid(vals []interface{}, bitWidths int32, pt parquet.Type) []byte {
	rleBuf := WriteRLE(vals, bitWidths, pt)
	res := make([]byte, 0)
	lenBuf := WritePlain([]interface{}{int32(len(rleBuf))}, parquet.Type_INT32)
	res = append(res, lenBuf...)
	res = append(res, rleBuf...)
	return res
}

func WriteRLEInt32(vals []int32, bitWidth int32) []byte {
	ln := len(vals)
	i := 0
	res := make([]byte, 0)
	for i < ln {
		j := i + 1
		for j < ln && vals[j] == vals[i] {
			j++
		}
		num := j - i
		header := num << 1
		byteNum := (bitWidth + 7) / 8
		headerBuf := WriteUnsignedVarInt(uint64(header))

		var valBuf [4]byte
		binary.LittleEndian.PutUint32(valBuf[:], uint32(vals[i]))

		res = append(res, headerBuf...)
		res = append(res, valBuf[:byteNum]...)
		i = j
	}
	return res
}

func WriteRLEBitPackedHybridInt32(vals []int32, bitWidths int32) []byte {
	rleBuf := WriteRLEInt32(vals, bitWidths)
	res := make([]byte, 0)
	lenBuf := WritePlain([]interface{}{int32(len(rleBuf))}, parquet.Type_INT32)
	res = append(res, lenBuf...)
	res = append(res, rleBuf...)
	return res
}

func WriteBitPacked(vals []interface{}, bitWidth int64, ifHeader bool) []byte {
	ln := len(vals)
	if ln <= 0 {
		return nil
	}
	valsInt := ToInt64(vals)

	header := ((ln/8)<<1 | 1)
	headerBuf := WriteUnsignedVarInt(uint64(header))

	valBuf := make([]byte, 0)

	i := 0
	var resCur int64 = 0
	var resCurNeedBits int64 = 8
	var used int64 = 0
	var left int64 = bitWidth - used
	val := int64(valsInt[i])
	for i < ln {
		if left >= resCurNeedBits {
			resCur |= ((val >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(8-resCurNeedBits)
			valBuf = append(valBuf, byte(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = 8
			resCur = 0

			if left <= 0 && (i+1) < ln {
				i += 1
				val = int64(valsInt[i])
				left = bitWidth
				used = 0
			}
		} else {
			resCur |= (val >> uint64(used)) << uint64(8-resCurNeedBits)
			i += 1

			if i < ln {
				val = int64(valsInt[i])
			}
			resCurNeedBits -= left

			left = bitWidth
			used = 0
		}
	}

	res := make([]byte, 0)
	if ifHeader {
		res = append(res, headerBuf...)
	}
	res = append(res, valBuf...)
	return res
}

func WriteDelta(nums []interface{}) []byte {
	ln := len(nums)
	if ln <= 0 {
		return []byte{}
	}

	if _, ok := nums[0].(int32); ok {
		return WriteDeltaINT32(nums)
	} else if _, ok := nums[0].(int64); ok {
		return WriteDeltaINT64(nums)
	} else {
		return []byte{}
	}
}

func WriteDeltaINT32(nums []interface{}) []byte {
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32
	var totalNumValues uint64 = uint64(len(nums))

	num := nums[0].(int32)
	var firstValue uint64 = uint64((num >> 31) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]interface{}, 0)
		var minDelta int32 = 0x7FFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := nums[i].(int32) - nums[i-1].(int32)
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			var maxValue int32 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(int32) - minDelta
				if blockBuf[k].(int32) > maxValue {
					maxValue = blockBuf[k].(int32)
				}
			}
			bitWidths[j] = byte(bits.Len32(uint32(maxValue)))
		}

		var minDeltaZigZag uint64 = uint64((minDelta >> 31) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			res = append(res, WriteBitPacked((blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock]), int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaINT64(nums []interface{}) []byte {
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32
	var totalNumValues uint64 = uint64(len(nums))

	num := nums[0].(int64)
	var firstValue uint64 = uint64((num >> 63) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]interface{}, 0)
		var minDelta int64 = 0x7FFFFFFFFFFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := nums[i].(int64) - nums[i-1].(int64)
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			var maxValue int64 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(int64) - minDelta
				if blockBuf[k].(int64) > maxValue {
					maxValue = blockBuf[k].(int64)
				}
			}
			bitWidths[j] = byte(bits.Len64(uint64(maxValue)))
		}

		var minDeltaZigZag uint64 = uint64((minDelta >> 63) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			res = append(res, WriteBitPacked((blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock]), int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaLengthByteArray(arrays []interface{}) []byte {
	ln := len(arrays)
	lengthArray := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		array := reflect.ValueOf(arrays[i]).String()
		lengthArray[i] = int32(len(array))
	}

	res := WriteDeltaINT32(lengthArray)

	for i := 0; i < ln; i++ {
		array := reflect.ValueOf(arrays[i]).String()
		res = append(res, array...)
	}
	return res
}

func WriteBitPackedDeprecated(vals []interface{}, bitWidth int64) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	valsInt := make([]uint64, ln)
	for i := 0; i < ln; i++ {
		valsInt[i] = uint64(reflect.ValueOf(vals[i]).Int())
	}

	res := make([]byte, 0)
	i := 0
	curByte := byte(0)
	var curNeed uint64 = 8
	var valBitLeft uint64 = uint64(bitWidth)
	var val uint64 = valsInt[0] << uint64(64-bitWidth)
	for i < ln {

		if valBitLeft > curNeed {
			var mask uint64 = ((1 << curNeed) - 1) << (64 - curNeed)

			curByte |= byte((val & mask) >> (64 - curNeed))
			val = val << curNeed

			valBitLeft -= curNeed
			res = append(res, curByte)
			curByte = byte(0)
			curNeed = 8

		} else {
			curByte |= byte(val >> (64 - curNeed))
			curNeed -= valBitLeft
			if curNeed == 0 {
				res = append(res, curByte)
				curByte = byte(0)
				curNeed = 8
			}

			valBitLeft = uint64(bitWidth)
			i++
			if i < ln {
				val = valsInt[i] << uint64(64-bitWidth)
			}
		}
	}
	return res
}

func WriteDeltaByteArray(arrays []interface{}) []byte {
	ln := len(arrays)
	if ln <= 0 {
		return []byte{}
	}

	prefixLengths := make([]interface{}, ln)
	suffixes := make([]interface{}, ln)
	prefixLengths[0] = int32(0)
	suffixes[0] = arrays[0]

	for i := 1; i < ln; i++ {
		s1 := reflect.ValueOf(arrays[i-1]).String()
		s2 := reflect.ValueOf(arrays[i]).String()
		l1 := len(s1)
		l2 := len(s2)
		j := 0
		for j < l1 && j < l2 {
			if s1[j] != s2[j] {
				break
			}
			j++
		}
		prefixLengths[i] = int32(j)
		suffixes[i] = (s2[j:])
	}

	prefixBuf := WriteDeltaINT32(prefixLengths)
	suffixBuf := WriteDeltaLengthByteArray(suffixes)

	res := make([]byte, 0)
	res = append(res, prefixBuf...)
	res = append(res, suffixBuf...)
	return res
}

func WriteByteStreamSplit(nums []interface{}) []byte {
	ln := len(nums)
	if ln <= 0 {
		return []byte{}
	}

	if _, ok := nums[0].(float32); ok {
		return WriteByteStreamSplitFloat32(nums)
	} else if _, ok := nums[0].(float64); ok {
		return WriteByteStreamSplitFloat64(nums)
	} else {
		return []byte{}
	}
}

func WriteByteStreamSplitFloat32(vals []interface{}) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*4)
	for i, n := range vals {
		v := math.Float32bits(n.(float32))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
	}
	return buf
}

func WriteByteStreamSplitFloat64(vals []interface{}) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}

	buf := make([]byte, ln*8)
	for i, n := range vals {
		v := math.Float64bits(n.(float64))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
		buf[ln*4+i] = byte(v >> 32)
		buf[ln*5+i] = byte(v >> 40)
		buf[ln*6+i] = byte(v >> 48)
		buf[ln*7+i] = byte(v >> 56)
	}
	return buf
}
