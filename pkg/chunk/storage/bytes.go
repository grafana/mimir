// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/storage/bytes.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storage

import (
	"bytes"
)

// Bytes exists to stop proto copying the byte array
type Bytes []byte

// Marshal just returns bs
func (bs *Bytes) Marshal() ([]byte, error) {
	return []byte(*bs), nil
}

// MarshalTo copies Bytes to data
func (bs *Bytes) MarshalTo(data []byte) (n int, err error) {
	return copy(data, *bs), nil
}

// Unmarshal updates Bytes to be data, without a copy
func (bs *Bytes) Unmarshal(data []byte) error {
	*bs = data
	return nil
}

// Size returns the length of Bytes
func (bs *Bytes) Size() int {
	return len(*bs)
}

// Equal returns true if other equals Bytes
func (bs *Bytes) Equal(other Bytes) bool {
	return bytes.Equal(*bs, other)
}

// Compare Bytes to other
func (bs *Bytes) Compare(other Bytes) int {
	return bytes.Compare(*bs, other)
}
