package common

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
)

// `parquet:"name=Name, type=FIXED_LEN_BYTE_ARRAY, length=12"`
type Tag struct {
	InName string
	ExName string

	Type      string
	KeyType   string
	ValueType string

	ConvertedType      string
	KeyConvertedType   string
	ValueConvertedType string

	Length      int32
	KeyLength   int32
	ValueLength int32

	Scale      int32
	KeyScale   int32
	ValueScale int32

	Precision      int32
	KeyPrecision   int32
	ValuePrecision int32

	IsAdjustedToUTC      bool
	KeyIsAdjustedToUTC   bool
	ValueIsAdjustedToUTC bool

	FieldID      int32
	KeyFieldID   int32
	ValueFieldID int32

	Encoding      parquet.Encoding
	KeyEncoding   parquet.Encoding
	ValueEncoding parquet.Encoding

	OmitStats      bool
	KeyOmitStats   bool
	ValueOmitStats bool

	RepetitionType      parquet.FieldRepetitionType
	KeyRepetitionType   parquet.FieldRepetitionType
	ValueRepetitionType parquet.FieldRepetitionType

	LogicalTypeFields      map[string]string
	KeyLogicalTypeFields   map[string]string
	ValueLogicalTypeFields map[string]string
}

func NewTag() *Tag {
	return &Tag{
		LogicalTypeFields:      make(map[string]string),
		KeyLogicalTypeFields:   make(map[string]string),
		ValueLogicalTypeFields: make(map[string]string),
	}
}

func StringToTag(tag string) (*Tag, error) {
	mp := NewTag()
	tagStr := strings.Replace(tag, "\t", "", -1)
	tags := strings.Split(tagStr, ",")

	for _, tag := range tags {
		tag = strings.TrimSpace(tag)

		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expect 'key=value' but got '%s'", tag)
		}
		key := kv[0]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)

		val := kv[1]
		val = strings.TrimSpace(val)

		var err error
		switch key {
		case "type":
			mp.Type = val
		case "keytype":
			mp.KeyType = val
		case "valuetype":
			mp.ValueType = val
		case "convertedtype":
			mp.ConvertedType = val
		case "keyconvertedtype":
			mp.KeyConvertedType = val
		case "valueconvertedtype":
			mp.ValueConvertedType = val
		case "length":
			if mp.Length, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse length: %s", err.Error())
			}
		case "keylength":
			if mp.KeyLength, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keylength: %s", err.Error())
			}
		case "valuelength":
			if mp.ValueLength, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuelength: %s", err.Error())
			}
		case "scale":
			if mp.Scale, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse scale: %s", err.Error())
			}
		case "keyscale":
			if mp.KeyScale, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyscale: %s", err.Error())
			}
		case "valuescale":
			if mp.ValueScale, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuescale: %s", err.Error())
			}
		case "precision":
			if mp.Precision, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse precision: %s", err.Error())
			}
		case "keyprecision":
			if mp.KeyPrecision, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyprecision: %s", err.Error())
			}
		case "valueprecision":
			if mp.ValuePrecision, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueprecision: %s", err.Error())
			}
		case "fieldid":
			if mp.FieldID, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse fieldid: %s", err.Error())
			}
		case "keyfieldid":
			if mp.KeyFieldID, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyfieldid: %s", err.Error())
			}
		case "valuefieldid":
			if mp.ValueFieldID, err = Str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuefieldid: %s", err.Error())
			}
		case "isadjustedtoutc":
			if mp.IsAdjustedToUTC, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse isadjustedtoutc: %s", err.Error())
			}
		case "keyisadjustedtoutc":
			if mp.KeyIsAdjustedToUTC, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyisadjustedtoutc: %s", err.Error())
			}
		case "valueisadjustedtoutc":
			if mp.ValueIsAdjustedToUTC, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueisadjustedtoutc: %s", err.Error())
			}
		case "name":
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
		case "inname":
			mp.InName = val
		case "omitstats":
			if mp.OmitStats, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse omitstats: %s", err.Error())
			}
		case "keyomitstats":
			if mp.KeyOmitStats, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyomitstats: %s", err.Error())
			}
		case "valueomitstats":
			if mp.ValueOmitStats, err = Str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueomitstats: %s", err.Error())
			}
		case "repetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.RepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown repetitiontype: '%v'", val)
			}
		case "keyrepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown keyrepetitiontype: '%v'", val)
			}
		case "valuerepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown valuerepetitiontype: '%v'", val)
			}
		case "encoding":
			switch strings.ToLower(val) {
			case "plain":
				mp.Encoding = parquet.Encoding_PLAIN
			case "rle":
				mp.Encoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.Encoding = parquet.Encoding_PLAIN_DICTIONARY
			case "rle_dictionary":
				mp.Encoding = parquet.Encoding_RLE_DICTIONARY
			case "byte_stream_split":
				mp.Encoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown encoding type: '%v'", val)
			}
		case "keyencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.KeyEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.KeyEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.KeyEncoding = parquet.Encoding_PLAIN_DICTIONARY
			case "byte_stream_split":
				mp.KeyEncoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown keyencoding type: '%v'", val)
			}
		case "valueencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.ValueEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.ValueEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.ValueEncoding = parquet.Encoding_PLAIN_DICTIONARY
			case "byte_stream_split":
				mp.ValueEncoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown valueencoding type: '%v'", val)
			}
		default:
			if strings.HasPrefix(key, "logicaltype") {
				mp.LogicalTypeFields[key] = val
			} else if strings.HasPrefix(key, "keylogicaltype") {
				newKey := key[3:]
				mp.KeyLogicalTypeFields[newKey] = val
			} else if strings.HasPrefix(key, "valuelogicaltype") {
				newKey := key[5:]
				mp.ValueLogicalTypeFields[newKey] = val
			} else {
				return nil, fmt.Errorf("unrecognized tag '%v'", key)
			}
		}
	}
	return mp, nil
}

func NewSchemaElementFromTagMap(info *Tag) (*parquet.SchemaElement, error) {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.FieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	if t, err := parquet.TypeFromString(info.Type); err == nil {
		schema.Type = &t

	} else {
		return nil, fmt.Errorf("type " + info.Type + ": " + err.Error())
	}

	if ct, err := parquet.ConvertedTypeFromString(info.ConvertedType); err == nil {
		schema.ConvertedType = &ct
	}

	var logicalType *parquet.LogicalType
	var err error
	if len(info.LogicalTypeFields) > 0 {
		logicalType, err = NewLogicalTypeFromFieldsMap(info.LogicalTypeFields)
		if err != nil {
			return nil, fmt.Errorf("failed to create logicaltype from field map: %s", err.Error())
		}
	} else {
		logicalType = NewLogicalTypeFromConvertedType(schema, info)
	}

	schema.LogicalType = logicalType

	return schema, nil
}

func NewLogicalTypeFromFieldsMap(mp map[string]string) (*parquet.LogicalType, error) {
	if val, ok := mp["logicaltype"]; !ok {
		return nil, errors.New("does not have logicaltype")
	} else {
		var err error
		logicalType := parquet.NewLogicalType()
		switch val {
		case "STRING":
			logicalType.STRING = parquet.NewStringType()
		case "MAP":
			logicalType.MAP = parquet.NewMapType()
		case "LIST":
			logicalType.LIST = parquet.NewListType()
		case "ENUM":
			logicalType.ENUM = parquet.NewEnumType()

		case "DECIMAL":
			logicalType.DECIMAL = parquet.NewDecimalType()
			logicalType.DECIMAL.Precision, err = Str2Int32(mp["logicaltype.precision"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.precision as int32: %s", err.Error())
			}
			logicalType.DECIMAL.Scale, err = Str2Int32(mp["logicaltype.scale"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.scale as int32: %s", err.Error())
			}

		case "DATE":
			logicalType.DATE = parquet.NewDateType()

		case "TIME":
			logicalType.TIME = parquet.NewTimeType()
			logicalType.TIME.IsAdjustedToUTC, err = Str2Bool(mp["logicaltype.isadjustedtoutc"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
			}
			switch mp["logicaltype.unit"] {
			case "MILLIS":
				logicalType.TIME.Unit = parquet.NewTimeUnit()
				logicalType.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
			case "MICROS":
				logicalType.TIME.Unit = parquet.NewTimeUnit()
				logicalType.TIME.Unit.MICROS = parquet.NewMicroSeconds()
			case "NANOS":
				logicalType.TIME.Unit = parquet.NewTimeUnit()
				logicalType.TIME.Unit.NANOS = parquet.NewNanoSeconds()
			default:
				return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", mp["logicaltype.unit"])
			}

		case "TIMESTAMP":
			logicalType.TIMESTAMP = parquet.NewTimestampType()
			logicalType.TIMESTAMP.IsAdjustedToUTC, err = Str2Bool(mp["logicaltype.isadjustedtoutc"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
			}
			switch mp["logicaltype.unit"] {
			case "MILLIS":
				logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
				logicalType.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
			case "MICROS":
				logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
				logicalType.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
			case "NANOS":
				logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
				logicalType.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
			default:
				return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", mp["logicaltype.unit"])
			}

		case "INTEGER":
			logicalType.INTEGER = parquet.NewIntType()
			bitWidth, err := Str2Int32(mp["logicaltype.bitwidth"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.bitwidth as int32: %s", err.Error())
			}
			logicalType.INTEGER.BitWidth = int8(bitWidth)
			logicalType.INTEGER.IsSigned, err = Str2Bool(mp["logicaltype.issigned"])
			if err != nil {
				return nil, fmt.Errorf("cannot parse logicaltype.issigned as boolean: %s", err.Error())
			}

		case "JSON":
			logicalType.JSON = parquet.NewJsonType()

		case "BSON":
			logicalType.BSON = parquet.NewBsonType()

		case "UUID":
			logicalType.UUID = parquet.NewUUIDType()

		default:
			return nil, fmt.Errorf("unknow logicaltype: " + val)
		}

		return logicalType, nil
	}
}

func NewLogicalTypeFromConvertedType(schemaElement *parquet.SchemaElement, info *Tag) *parquet.LogicalType {
	_, ct := schemaElement.Type, schemaElement.ConvertedType
	if ct == nil {
		return nil
	}

	logicalType := parquet.NewLogicalType()
	switch *ct {
	case parquet.ConvertedType_INT_8:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 8
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_16:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 16
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_32:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 32
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_64:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 64
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_UINT_8:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 8
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_16:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 16
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_32:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 32
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_64:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 64
		logicalType.INTEGER.IsSigned = false

	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision = info.Precision
		logicalType.DECIMAL.Scale = info.Scale

	case parquet.ConvertedType_DATE:
		logicalType.DATE = parquet.NewDateType()

	case parquet.ConvertedType_TIME_MICROS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIME.Unit = parquet.NewTimeUnit()
		logicalType.TIME.Unit.MICROS = parquet.NewMicroSeconds()

	case parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIME.Unit = parquet.NewTimeUnit()
		logicalType.TIME.Unit.MILLIS = parquet.NewMilliSeconds()

	case parquet.ConvertedType_TIMESTAMP_MICROS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
		logicalType.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()

	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
		logicalType.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()

	case parquet.ConvertedType_BSON:
		logicalType.BSON = parquet.NewBsonType()

	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = parquet.NewEnumType()

	case parquet.ConvertedType_JSON:
		logicalType.JSON = parquet.NewJsonType()

	case parquet.ConvertedType_LIST:
		logicalType.LIST = parquet.NewListType()

	case parquet.ConvertedType_MAP:
		logicalType.MAP = parquet.NewMapType()

	case parquet.ConvertedType_UTF8:
		logicalType.STRING = parquet.NewStringType()

	default:
		return nil
	}

	return logicalType
}

func DeepCopy(src, dst interface{}) {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(src)
	gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
	return
}

//Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Key"
	res.ExName = "key"
	res.Type = src.KeyType
	res.ConvertedType = src.KeyConvertedType
	res.IsAdjustedToUTC = src.KeyIsAdjustedToUTC
	res.Length = src.KeyLength
	res.Scale = src.KeyScale
	res.Precision = src.KeyPrecision
	res.FieldID = src.KeyFieldID
	res.Encoding = src.KeyEncoding
	res.OmitStats = src.KeyOmitStats
	res.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	return res
}

//Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Value"
	res.ExName = "value"
	res.Type = src.ValueType
	res.ConvertedType = src.ValueConvertedType
	res.IsAdjustedToUTC = src.ValueIsAdjustedToUTC
	res.Length = src.ValueLength
	res.Scale = src.ValueScale
	res.Precision = src.ValuePrecision
	res.FieldID = src.ValueFieldID
	res.Encoding = src.ValueEncoding
	res.OmitStats = src.ValueOmitStats
	res.RepetitionType = src.ValueRepetitionType
	return res
}

//Convert string to a golang variable name
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := 0; i < ln; i++ {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			name += string(c)

		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = HeadToUpper(name)
	return name
}

//Convert the first letter of a string to uppercase
func HeadToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	//handle non-alpha prefix such as "_"
	return "PARGO_PREFIX_" + str
}

func CmpIntBinary(as string, bs string, order string, signed bool) bool {
	abs, bbs := []byte(as), []byte(bs)
	la, lb := len(abs), len(bbs)

	if order == "LittleEndian" {
		for i, j := 0, len(abs)-1; i < j; i, j = i+1, j-1 {
			abs[i], abs[j] = abs[j], abs[i]
		}
		for i, j := 0, len(bbs)-1; i < j; i, j = i+1, j-1 {
			bbs[i], bbs[j] = bbs[j], bbs[i]
		}
	}
	if !signed {
		if la < lb {
			abs = append(make([]byte, lb-la), abs...)
		} else if lb < la {
			bbs = append(make([]byte, la-lb), bbs...)
		}
	} else {
		if la < lb {
			sb := (abs[0] >> 7) & 1
			pre := make([]byte, lb-la)
			if sb == 1 {
				for i := 0; i < lb-la; i++ {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)

		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := 0; i < la-lb; i++ {
					pre[i] = byte(0xFF)
				}
			}
			bbs = append(pre, bbs...)
		}

		asb, bsb := (abs[0]>>7)&1, (bbs[0]>>7)&1

		if asb < bsb {
			return false
		} else if asb > bsb {
			return true
		}

	}

	for i := 0; i < len(abs); i++ {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}

func FindFuncTable(pT *parquet.Type, cT *parquet.ConvertedType, logT *parquet.LogicalType) FuncTable {
	if cT == nil && logT == nil {
		if *pT == parquet.Type_BOOLEAN {
			return boolFuncTable{}
		} else if *pT == parquet.Type_INT32 {
			return int32FuncTable{}
		} else if *pT == parquet.Type_INT64 {
			return int64FuncTable{}
		} else if *pT == parquet.Type_INT96 {
			return int96FuncTable{}
		} else if *pT == parquet.Type_FLOAT {
			return float32FuncTable{}
		} else if *pT == parquet.Type_DOUBLE {
			return float64FuncTable{}
		} else if *pT == parquet.Type_BYTE_ARRAY {
			return stringFuncTable{}
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return stringFuncTable{}
		}
	}

	if cT != nil {
		if *cT == parquet.ConvertedType_UTF8 || *cT == parquet.ConvertedType_BSON || *cT == parquet.ConvertedType_JSON {
			return stringFuncTable{}
		} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
			*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
			return int32FuncTable{}
		} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
			return uint32FuncTable{}
		} else if *cT == parquet.ConvertedType_INT_64 || *cT == parquet.ConvertedType_TIME_MICROS ||
			*cT == parquet.ConvertedType_TIMESTAMP_MILLIS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
			return int64FuncTable{}
		} else if *cT == parquet.ConvertedType_UINT_64 {
			return uint64FuncTable{}
		} else if *cT == parquet.ConvertedType_INTERVAL {
			return intervalFuncTable{}
		} else if *cT == parquet.ConvertedType_DECIMAL {
			if *pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return decimalStringFuncTable{}
			} else if *pT == parquet.Type_INT32 {
				return int32FuncTable{}
			} else if *pT == parquet.Type_INT64 {
				return int64FuncTable{}
			}
		}
	}

	if logT != nil {
		if logT.TIME != nil || logT.TIMESTAMP != nil {
			return FindFuncTable(pT, nil, nil)

		} else if logT.DATE != nil {
			return int32FuncTable{}

		} else if logT.INTEGER != nil {
			if logT.INTEGER.IsSigned {
				return FindFuncTable(pT, nil, nil)

			} else {
				if *pT == parquet.Type_INT32 {
					return uint32FuncTable{}

				} else if *pT == parquet.Type_INT64 {
					return uint64FuncTable{}
				}
			}

		} else if logT.DECIMAL != nil {
			if *pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return decimalStringFuncTable{}
			} else if *pT == parquet.Type_INT32 {
				return int32FuncTable{}
			} else if *pT == parquet.Type_INT64 {
				return int64FuncTable{}
			}

		} else if logT.BSON != nil || logT.JSON != nil || logT.STRING != nil || logT.UUID != nil {
			return stringFuncTable{}
		}
	}

	panic("No known func table in FindFuncTable")
}

func Str2Int32(val string) (int32, error) {
	valInt, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return int32(valInt), nil
}

func Str2Bool(val string) (bool, error) {
	valBoolean, err := strconv.ParseBool(val)
	if err != nil {
		return false, err
	}
	return valBoolean, nil
}

type FuncTable interface {
	LessThan(a interface{}, b interface{}) bool
	MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32)
}

func Min(table FuncTable, a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return a
	} else {
		return b
	}
}

func Max(table FuncTable, a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return b
	} else {
		return a
	}
}

type boolFuncTable struct{}

func (_ boolFuncTable) LessThan(a interface{}, b interface{}) bool {
	return !a.(bool) && b.(bool)
}

func (table boolFuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 1
}

type int32FuncTable struct{}

func (_ int32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(int32) < b.(int32)
}

func (table int32FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type uint32FuncTable struct{}

func (_ uint32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return uint32(a.(int32)) < uint32(b.(int32))
}

func (table uint32FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type int64FuncTable struct{}

func (_ int64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(int64) < b.(int64)
}

func (table int64FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type uint64FuncTable struct{}

func (_ uint64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return uint64(a.(int64)) < uint64(b.(int64))
}

func (table uint64FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type int96FuncTable struct{}

func (_ int96FuncTable) LessThan(ai interface{}, bi interface{}) bool {
	a, b := []byte(ai.(string)), []byte(bi.(string))
	fa, fb := a[11]>>7, b[11]>>7
	if fa > fb {
		return true
	} else if fa < fb {
		return false
	}
	for i := 11; i >= 0; i-- {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (table int96FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type float32FuncTable struct{}

func (_ float32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(float32) < b.(float32)
}

func (table float32FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type float64FuncTable struct{}

func (_ float64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(float64) < b.(float64)
}

func (table float64FuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type stringFuncTable struct{}

func (_ stringFuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(string) < b.(string)
}

func (table stringFuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type intervalFuncTable struct{}

func (_ intervalFuncTable) LessThan(ai interface{}, bi interface{}) bool {
	a, b := []byte(ai.(string)), []byte(bi.(string))
	for i := 11; i >= 0; i-- {
		if a[i] > b[i] {
			return false
		} else if a[i] < b[i] {
			return true
		}
	}
	return false
}

func (table intervalFuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type decimalStringFuncTable struct{}

func (_ decimalStringFuncTable) LessThan(a interface{}, b interface{}) bool {
	return CmpIntBinary(a.(string), b.(string), "BigEndian", true)
}

func (table decimalStringFuncTable) MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

//Get the size of a parquet value
func SizeOf(val reflect.Value) int64 {
	var size int64
	switch val.Type().Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := 0; i < val.Type().NumField(); i++ {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	case reflect.Bool:
		return 1
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.String:
		return int64(val.Len())
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	}
	return 4
}

const PAR_GO_PATH_DELIMITER = "\x01"

// . -> \x01
func ReformPathStr(pathStr string) string {
	return strings.ReplaceAll(pathStr, ".", "\x01")
}

//Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, PAR_GO_PATH_DELIMITER)
}

//Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, PAR_GO_PATH_DELIMITER)
}

//Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, PAR_GO_PATH_DELIMITER))
}

// NewTable creates empty table with transposed columns and records
func NewTable(rowLen, colLen int) [][]interface{} {
	tableLen := make([]interface{}, rowLen*colLen)
	// Need to reconsinder to avoid allocation and memcopy.
	newTable := make([][]interface{}, rowLen)
	lo, hi := 0, colLen
	for i := range newTable {
		newTable[i] = tableLen[lo:hi:hi]
		lo, hi = hi, hi+colLen
	}
	return newTable
}

// TransposeTable transposes a table's rows and columns once per arrow record.
// We need to transpose the rows and columns because parquet-go library writes
// data row by row while the arrow library provides the data column by column.
func TransposeTable(table [][]interface{}) [][]interface{} {
	transposedTable := NewTable(len(table[0]), len(table))
	for i := 0; i < len(transposedTable); i++ {
		row := transposedTable[i]
		for j := 0; j < len(row); j++ {
			row[j] = table[j][i]
		}
	}
	return transposedTable
}

// ArrowColToParquetCol creates column with native parquet values from column
// with arrow values.
//
// If a single record is not valid by the arrow definitions we assign it
// default value which we chose.
func ArrowColToParquetCol(field arrow.Field, col array.Interface, len int,
	el *parquet.SchemaElement) ([]interface{}, error) {
	var err error
	recs := make([]interface{}, len)
	switch field.Type.(type) {
	case *arrow.Int8Type:
		arr := col.(*array.Int8)
		var rec int8
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Int16Type:
		arr := col.(*array.Int16)
		var rec int16
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Int32Type:
		arr := col.(*array.Int32)
		var rec int32
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Int64Type:
		arr := col.(*array.Int64)
		var rec int64
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.Uint8Type:
		arr := col.(*array.Uint8)
		var rec uint8
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Uint16Type:
		arr := col.(*array.Uint16)
		var rec uint16
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.Uint32Type:
		arr := col.(*array.Uint32)
		var rec int32
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = int32(arr.Value(i))
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.Uint64Type:
		arr := col.(*array.Uint64)
		var rec int64
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = int64(arr.Value(i))
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.Float32Type:
		arr := col.(*array.Float32)
		var rec float32
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Float64Type:
		arr := col.(*array.Float64)
		var rec float64
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Date32Type:
		arr := col.(*array.Date32)
		var rec arrow.Date32
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.Date64Type:
		arr := col.(*array.Date64)
		var rec arrow.Date64
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.BinaryType:
		arr := col.(*array.Binary)
		var rec []byte
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = []byte("")
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.StringType:
		arr := col.(*array.String)
		var rec string
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = ""
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))

			if err != nil {
				return nil, err
			}
		}
	case *arrow.BooleanType:
		arr := col.(*array.Boolean)
		var rec bool
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = false
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.Time32Type:
		arr := col.(*array.Time32)
		var rec arrow.Time32
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	case *arrow.TimestampType:
		arr := col.(*array.Timestamp)
		var rec arrow.Timestamp
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				rec = arr.Value(i)
			} else {
				rec = 0
			}
			recs[i], err = types.StrToParquetType(fmt.Sprintf("%v", rec),
				el.Type,
				el.ConvertedType,
				int(el.GetTypeLength()),
				int(el.GetScale()))
			if err != nil {
				return nil, err
			}
		}
	}
	return recs, nil
}
