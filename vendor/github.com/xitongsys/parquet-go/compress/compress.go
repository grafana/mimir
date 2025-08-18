package compress

import (
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
)

type Compressor struct {
	Compress   func(buf []byte) []byte
	Uncompress func(buf []byte) ([]byte, error)
}

var compressors = map[parquet.CompressionCodec]*Compressor{}

func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, fmt.Errorf("unsupported compress method")
	}

	return c.Uncompress(buf)
}

func Compress(buf []byte, compressMethod parquet.CompressionCodec) []byte {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil
	}
	return c.Compress(buf)
}
