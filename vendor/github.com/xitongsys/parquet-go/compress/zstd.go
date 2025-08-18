//go:build !no_zstd
// +build !no_zstd

package compress

import (
	"github.com/klauspost/compress/zstd"
	"github.com/xitongsys/parquet-go/parquet"
)

func init() {
	// Create encoder/decoder with default parameters.
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	dec, _ := zstd.NewReader(nil)
	compressors[parquet.CompressionCodec_ZSTD] = &Compressor{
		Compress: func(buf []byte) []byte {
			return enc.EncodeAll(buf, nil)
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			return dec.DecodeAll(buf, nil)
		},
	}
}
