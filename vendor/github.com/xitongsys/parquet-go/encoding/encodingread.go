package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/xitongsys/parquet-go/parquet"
)

func ReadPlain(bytesReader *bytes.Reader, dataType parquet.Type, cnt uint64, bitWidth uint64) ([]interface{}, error) {
	if dataType == parquet.Type_BOOLEAN {
		return ReadPlainBOOLEAN(bytesReader, cnt)
	} else if dataType == parquet.Type_INT32 {
		return ReadPlainINT32(bytesReader, cnt)
	} else if dataType == parquet.Type_INT64 {
		return ReadPlainINT64(bytesReader, cnt)
	} else if dataType == parquet.Type_INT96 {
		return ReadPlainINT96(bytesReader, cnt)
	} else if dataType == parquet.Type_FLOAT {
		return ReadPlainFLOAT(bytesReader, cnt)
	} else if dataType == parquet.Type_DOUBLE {
		return ReadPlainDOUBLE(bytesReader, cnt)
	} else if dataType == parquet.Type_BYTE_ARRAY {
		return ReadPlainBYTE_ARRAY(bytesReader, cnt)
	} else if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, cnt, bitWidth)
	} else {
		return nil, fmt.Errorf("Unknown parquet type")
	}
}

func ReadPlainBOOLEAN(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	res = make([]interface{}, cnt)
	resInt, err := ReadBitPacked(bytesReader, uint64(cnt<<1), 1)
	if err != nil {
		return res, err
	}

	for i := 0; i < int(cnt); i++ {
		if resInt[i].(int64) > 0 {
			res[i] = true
		} else {
			res[i] = false
		}
	}
	return res, err
}

func ReadPlainINT32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT32(bytesReader, res)
	return res, err
}

func ReadPlainINT64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT64(bytesReader, res)
	return res, err
}

func ReadPlainINT96(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	cur := make([]byte, 12)
	for i := 0; i < int(cnt); i++ {
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur[:12])
	}
	return res, err
}

func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT32(bytesReader, res)
	return res, err
}

func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT64(bytesReader, res)
	return res, err
}

func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		if _, err = bytesReader.Read(buf); err != nil {
			break
		}
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		bytesReader.Read(cur)
		res[i] = string(cur)
	}
	return res, err
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64, fixedLength uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		cur := make([]byte, fixedLength)
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur)
	}
	return res, err
}

func ReadUnsignedVarInt(bytesReader *bytes.Reader) (uint64, error) {
	var err error
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			break
		}
		res |= ((uint64(b) & uint64(0x7F)) << uint64(shift))
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res, err
}

//RLE return res is []INT64
func ReadRLE(bytesReader *bytes.Reader, header uint64, bitWidth uint64) ([]interface{}, error) {
	var err error
	var res []interface{}
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = bytesReader.Read(data); err != nil {
			return res, err
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	res = make([]interface{}, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res, err
}

//return res is []INT64
func ReadBitPacked(bytesReader *bytes.Reader, header uint64, bitWidth uint64) ([]interface{}, error) {
	var err error
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	res := make([]interface{}, 0, cnt)

	if cnt == 0 {
		return res, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, int64(0))
		}
		return res, err
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = bytesReader.Read(bytesBuf); err != nil {
		return res, err
	}

	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = bytesBuf[i]
				left = 8
				used = 0
			}

		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i += 1
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}
	return res, err
}

//res is INT64
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth uint64, length uint64) ([]interface{}, error) {
	res := make([]interface{}, 0)
	if length <= 0 {
		lb, err := ReadPlainINT32(bytesReader, 1)
		if err != nil {
			return res, err
		}
		length = uint64(lb[0].(int32))
	}

	buf := make([]byte, length)
	if _, err := bytesReader.Read(buf); err != nil {
		return res, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := ReadUnsignedVarInt(newReader)
		if err != nil {
			return res, err
		}
		if header&1 == 0 {
			buf, err := ReadRLE(newReader, header, bitWidth)
			if err != nil {
				return res, err
			}
			res = append(res, buf...)

		} else {
			buf, err := ReadBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, err
			}
			res = append(res, buf...)
		}
	}
	return res, nil
}

func ReadDeltaBinaryPackedINT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}

	fv32 := int32(firstValueZigZag)
	var firstValue int32 = int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}

		md32 := int32(minDeltaZigZag)
		var minDelta int32 = int32(uint32(md32)>>1) ^ -(md32 & 1)
		var bitWidths = make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, err
			}
			bitWidths[i] = uint64(b)
		}
		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, err
			}
			for j := 0; j < len(cur) && len(res) < int(numValues); j++ {
				res = append(res, int32(res[len(res)-1].(int32)+int32(cur[j].(int64))+minDelta))
			}
		}
	}
	return res[:numValues], err
}

//res is INT64
func ReadDeltaBinaryPackedINT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	var firstValue int64 = int64(firstValueZigZag>>1) ^ -(int64(firstValueZigZag) & 1)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, int64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}
		var minDelta int64 = int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
		var bitWidths = make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, err
			}
			bitWidths[i] = uint64(b)
		}

		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, err
			}
			for j := 0; j < len(cur); j++ {
				res = append(res, (res[len(res)-1].(int64) + cur[j].(int64) + minDelta))
			}
		}
	}
	return res[:numValues], err
}

func ReadDeltaLengthByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	lengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, err
	}
	res = make([]interface{}, len(lengths))
	for i := 0; i < len(lengths); i++ {
		res[i] = ""
		length := uint64(lengths[i].(int64))
		if length > 0 {
			cur, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, 1, length)
			if err != nil {
				return res, err
			}
			res[i] = cur[0]
		}
	}

	return res, err
}

func ReadDeltaByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	prefixLengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, err
	}
	suffixes, err := ReadDeltaLengthByteArray(bytesReader)
	if err != nil {
		return res, err
	}
	res = make([]interface{}, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(int64)
		prefix := res[i-1].(string)[:prefixLength]
		suffix := suffixes[i].(string)
		res[i] = prefix + suffix
	}
	return res, err
}

func ReadByteStreamSplitFloat32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {

	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*4)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*4 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float32frombits(uint32(buf[i]) |
			uint32(buf[cnt+i])<<8 |
			uint32(buf[cnt*2+i])<<16 |
			uint32(buf[cnt*3+i])<<24)
	}

	return res, err
}

func ReadByteStreamSplitFloat64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {

	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*8)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*8 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float64frombits(uint64(buf[i]) |
			uint64(buf[cnt+i])<<8 |
			uint64(buf[cnt*2+i])<<16 |
			uint64(buf[cnt*3+i])<<24 |
			uint64(buf[cnt*4+i])<<32 |
			uint64(buf[cnt*5+i])<<40 |
			uint64(buf[cnt*6+i])<<48 |
			uint64(buf[cnt*7+i])<<56)
	}

	return res, err
}
