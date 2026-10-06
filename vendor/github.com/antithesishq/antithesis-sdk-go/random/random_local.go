//go:build !enable_antithesis_sdk

package random

import (
	crand "crypto/rand"
	"encoding/binary"
)

func GetRandom() uint64 {
	var tmp [8]byte
	crand.Read(tmp[:])
	return binary.LittleEndian.Uint64(tmp[:])
}
