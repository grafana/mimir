// SPDX-License-Identifier: AGPL-3.0-only

package util

import "unsafe"

func YoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
