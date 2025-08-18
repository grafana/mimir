package types

import (
	"fmt"
	"math/big"
	"reflect"

	"github.com/xitongsys/parquet-go/parquet"
)

func ParquetTypeToGoReflectType(pT *parquet.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	if rT == nil || *rT != parquet.FieldRepetitionType_OPTIONAL {
		if *pT == parquet.Type_BOOLEAN {
			return reflect.TypeOf(true)

		} else if *pT == parquet.Type_INT32 {
			return reflect.TypeOf(int32(0))

		} else if *pT == parquet.Type_INT64 {
			return reflect.TypeOf(int64(0))

		} else if *pT == parquet.Type_INT96 {
			return reflect.TypeOf("")

		} else if *pT == parquet.Type_FLOAT {
			return reflect.TypeOf(float32(0))

		} else if *pT == parquet.Type_DOUBLE {
			return reflect.TypeOf(float64(0))

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return reflect.TypeOf("")

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return reflect.TypeOf("")

		} else {
			return nil
		}

	} else {
		if *pT == parquet.Type_BOOLEAN {
			v := true
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_INT32 {
			v := int32(0)
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_INT64 {
			v := int64(0)
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_INT96 {
			v := ""
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_FLOAT {
			v := float32(0)
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_DOUBLE {
			v := float64(0)
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_BYTE_ARRAY {
			v := ""
			return reflect.TypeOf(&v)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			v := ""
			return reflect.TypeOf(&v)

		} else {
			return nil
		}
	}
}

//Scan a string to parquet value; length and scale just for decimal
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length int, scale int) (interface{}, error) {
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			var v bool
			_, err := fmt.Sscanf(s, "%t", &v)
			return v, err

		} else if *pT == parquet.Type_INT32 {
			var v int32
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, err

		} else if *pT == parquet.Type_INT64 {
			var v int64
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, err

		} else if *pT == parquet.Type_INT96 {
			res := StrIntToBinary(s, "LittleEndian", 12, true)
			return res, nil

		} else if *pT == parquet.Type_FLOAT {
			var v float32
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, err

		} else if *pT == parquet.Type_DOUBLE {
			var v float64
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, err

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return s, nil

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return s, nil
		}
		return nil, nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return s, nil

	} else if *cT == parquet.ConvertedType_INT_8 {
		var v int8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_INT_16 {
		var v int16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_INT_32 {
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_UINT_8 {
		var v uint8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_UINT_16 {
		var v uint16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_UINT_32 {
		var v uint32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err

	} else if *cT == parquet.ConvertedType_UINT_64 {
		var vt uint64
		_, err := fmt.Sscanf(s, "%d", &vt)
		return int64(vt), err

	} else if *cT == parquet.ConvertedType_INT_64 ||
		*cT == parquet.ConvertedType_TIME_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err

	} else if *cT == parquet.ConvertedType_INTERVAL {
		res := StrIntToBinary(s, "LittleEndian", 12, false)
		return res, nil

	} else if *cT == parquet.ConvertedType_DECIMAL {
		numSca := big.NewFloat(1.0)
		for i := 0; i < scale; i++ {
			numSca.Mul(numSca, big.NewFloat(10))
		}
		num := new(big.Float)
		num.SetString(s)
		num.Mul(num, numSca)

		if *pT == parquet.Type_INT32 {
			tmp, _ := num.Float64()
			return int32(tmp), nil

		} else if *pT == parquet.Type_INT64 {
			tmp, _ := num.Float64()
			return int64(tmp), nil

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			s = num.Text('f', 0)
			res := StrIntToBinary(s, "BigEndian", length, true)
			return res, nil

		} else {
			s = num.Text('f', 0)
			res := StrIntToBinary(s, "BigEndian", 0, true)
			return res, nil
		}
	} else {
		return nil, nil
	}
}

func InterfaceToParquetType(src interface{}, pT *parquet.Type) interface{} {
	if src == nil {
		return src
	}

	if pT == nil {
		return src
	}

	switch *pT {
	case parquet.Type_BOOLEAN:
		if _, ok := src.(bool); ok {
			return src
		} else {
			return reflect.ValueOf(src).Bool()
		}

	case parquet.Type_INT32:
		if _, ok := src.(int32); ok {
			return src
		} else {
			return int32(reflect.ValueOf(src).Int())
		}

	case parquet.Type_INT64:
		if _, ok := src.(int64); ok {
			return src
		} else {
			return reflect.ValueOf(src).Int()
		}

	case parquet.Type_FLOAT:
		if _, ok := src.(float32); ok {
			return src
		} else {
			return float32(reflect.ValueOf(src).Float())
		}

	case parquet.Type_DOUBLE:
		if _, ok := src.(float64); ok {
			return src
		} else {
			return reflect.ValueOf(src).Float()
		}

	case parquet.Type_INT96:
		fallthrough
	case parquet.Type_BYTE_ARRAY:
		fallthrough
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if _, ok := src.(string); ok {
			return src
		} else {
			return reflect.ValueOf(src).String()
		}

	default:
		return src
	}
}

//order=LittleEndian or BigEndian; length is byte num
func StrIntToBinary(num string, order string, length int, signed bool) string {
	bigNum := new(big.Int)
	bigNum.SetString(num, 10)
	if !signed {
		res := bigNum.Bytes()
		if len(res) < length {
			res = append(make([]byte, length-len(res)), res...)
		}
		if order == "LittleEndian" {
			for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
				res[i], res[j] = res[j], res[i]
			}
		}
		if length > 0 {
			res = res[len(res)-length:]
		}
		return string(res)
	}

	flag := bigNum.Cmp(big.NewInt(0))
	if flag == 0 {
		if length <= 0 {
			length = 1
		}
		return string(make([]byte, length))
	}

	bigNum = bigNum.SetBytes(bigNum.Bytes())
	bs := bigNum.Bytes()

	if len(bs) < length {
		bs = append(make([]byte, length-len(bs)), bs...)
	}

	upperBs := make([]byte, len(bs))
	upperBs[0] = byte(0x80)
	upper := new(big.Int)
	upper.SetBytes(upperBs)
	if flag > 0 {
		upper = upper.Sub(upper, big.NewInt(1))
	}

	if bigNum.Cmp(upper) > 0 {
		bs = append(make([]byte, 1), bs...)
	}

	if flag < 0 {
		modBs := make([]byte, len(bs)+1)
		modBs[0] = byte(0x01)
		mod := new(big.Int)
		mod.SetBytes(modBs)
		bs = mod.Sub(mod, bigNum).Bytes()
	}
	if length > 0 {
		bs = bs[len(bs)-length:]
	}
	if order == "LittleEndian" {
		for i, j := 0, len(bs)-1; i < j; i, j = i+1, j-1 {
			bs[i], bs[j] = bs[j], bs[i]
		}
	}
	return string(bs)
}

func JSONTypeToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, length int, scale int) (interface{}, error) {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil, nil
	}
	s := fmt.Sprintf("%v", val)
	return StrToParquetType(s, pT, cT, length, scale)
}
