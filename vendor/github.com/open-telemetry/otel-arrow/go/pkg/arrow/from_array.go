/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

// Utility functions to extract values from Arrow arrays.

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// I32FromArray returns the int32 value for a specific row in an Arrow array.
// This Arrow array can be either an int32 array or a dictionary-encoded array.
func I32FromArray(arr arrow.Array, row int) (int32, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Int32:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		case *array.Dictionary:
			i32Arr := arr.Dictionary().(*array.Int32)
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return i32Arr.Value(arr.GetValueIndex(row)), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not an int32 array")
		}
	}
}

// I64FromArray returns the int64 value for a specific row in an Arrow array.
// This Arrow array can be either an int64 array or a dictionary-encoded int64 array.
func I64FromArray(arr arrow.Array, row int) (int64, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Int64:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		case *array.Dictionary:
			i64Arr := arr.Dictionary().(*array.Int64)
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return i64Arr.Value(arr.GetValueIndex(row)), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not an int64 array")
		}
	}
}

// StringFromArray returns the string value for a specific row in an Arrow array.
func StringFromArray(arr arrow.Array, row int) (string, error) {
	if arr == nil {
		return "", nil
	} else {
		if arr.IsNull(row) {
			return "", nil
		}

		switch arr := arr.(type) {
		case *array.String:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.String).Value(arr.GetValueIndex(row)), nil
		default:
			return "", werror.WrapWithContext(ErrInvalidArrayType, map[string]interface{}{
				"message":    "invalid array type",
				"array-type": arr.DataType().Name(),
			})
		}
	}
}

// BinaryFromArray returns the binary value for a specific row in an Arrow array.
func BinaryFromArray(arr arrow.Array, row int) ([]byte, error) {
	if arr == nil {
		return nil, nil
	} else {
		if arr.IsNull(row) {
			return nil, nil
		}

		switch arr := arr.(type) {
		case *array.Binary:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.Binary).Value(arr.GetValueIndex(row)), nil
		default:
			return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a binary array")
		}
	}
}

// FixedSizeBinaryFromArray returns the fixed size binary value for a specific row in an Arrow array.
func FixedSizeBinaryFromArray(arr arrow.Array, row int) ([]byte, error) {
	if arr == nil {
		return nil, nil
	} else {
		if arr.IsNull(row) {
			return nil, nil
		}

		switch arr := arr.(type) {
		case *array.FixedSizeBinary:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.FixedSizeBinary).Value(arr.GetValueIndex(row)), nil
		default:
			return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a fixed size binary array")
		}
	}
}

// BoolFromArray returns the bool value for a specific row in an Arrow array.
func BoolFromArray(arr arrow.Array, row int) (bool, error) {
	if arr == nil {
		return false, nil
	} else {
		switch arr := arr.(type) {
		case *array.Boolean:
			if arr.IsNull(row) {
				return false, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return false, werror.WrapWithMsg(ErrInvalidArrayType, "not a bool array")
		}
	}
}

// F64FromArray returns the float64 value for a specific row in an Arrow array.
func F64FromArray(arr arrow.Array, row int) (float64, error) {
	if arr == nil {
		return 0.0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Float64:
			if arr.IsNull(row) {
				return 0.0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0.0, werror.WrapWithMsg(ErrInvalidArrayType, "not a float64 array")
		}
	}
}

// F64OrNilFromArray returns a pointer to the float64 value for a specific row in an Arrow array or nil if the value is nil.
func F64OrNilFromArray(arr arrow.Array, row int) (*float64, error) {
	if arr == nil {
		return nil, nil
	} else {
		switch arr := arr.(type) {
		case *array.Float64:
			if arr.IsNull(row) {
				return nil, nil
			} else {
				v := arr.Value(row)
				return &v, nil
			}
		default:
			return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a float64 array")
		}
	}
}

// U64FromArray returns the uint64 value for a specific row in an Arrow array.
func U64FromArray(arr arrow.Array, row int) (uint64, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint64:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint64 array")
		}
	}
}

// TimestampFromArray returns the timestamp value for a specific row in an Arrow array.
func TimestampFromArray(arr arrow.Array, row int) (arrow.Timestamp, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Timestamp:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a timestamp array")
		}
	}
}

// DurationFromArray returns the duration value for a specific row in an Arrow array.
// This Arrow array can be either a duration array or a dictionary array.
func DurationFromArray(arr arrow.Array, row int) (arrow.Duration, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Duration:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		case *array.Dictionary:
			durationArr := arr.Dictionary().(*array.Duration)
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return durationArr.Value(arr.GetValueIndex(row)), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a duration array")
		}
	}
}

// U8FromArray returns the uint8 value for a specific row in an Arrow array.
func U8FromArray(arr arrow.Array, row int) (uint8, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint8:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint8 array")
		}
	}
}

// U16FromArray returns the uint16 value for a specific row in an Arrow array.
func U16FromArray(arr arrow.Array, row int) (uint16, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint16:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint16 array")
		}
	}
}

// U32FromArray returns the uint32 value for a specific row in an Arrow array.
func U32FromArray(arr arrow.Array, row int) (uint32, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint32:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint32 array")
		}
	}
}
