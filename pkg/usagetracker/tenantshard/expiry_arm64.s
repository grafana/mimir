// SPDX-License-Identifier: AGPL-3.0-only

//go:build arm64 && !nosimd

#include "textflag.h"

// func scanExpired(p *data, n int, lo, length uint8) int
//
// Evaluates matchExpiredWord over two groups at a time: subtract lo from every byte lane,
// keep the lanes that land within length of it, and drop the lanes holding the empty or
// tombstone markers.
//
// The unsigned byte comparisons are expressed with UMIN and UMAX rather than with CMHS and
// CMHI, because the assembler only learned the latter in Go 1.27: "y <= n" is
// "umin(y, n) == y" and "x >= 2" is "umax(x, 2) == x". Two extra instructions per iteration,
// and it assembles on the Go version Mimir currently builds with.
TEXT ·scanExpired(SB), NOSPLIT|NOFRAME, $0-32
	MOVD	p+0(FP), R0
	MOVD	n+8(FP), R1
	MOVBU	lo+16(FP), R2
	MOVBU	length+17(FP), R3

	VDUP	R2, V0.B16              // lo in every lane
	VDUP	R3, V1.B16              // length in every lane
	MOVD	$2, R4
	VDUP	R4, V2.B16              // lowest occupied byte value in every lane

	MOVD	$0, R5                  // index of the group under the cursor

loop16:
	CMP	$2, R1
	BLT	tail
	VLD1	(R0), [V4.B16]
	VSUB	V0.B16, V4.B16, V5.B16  // V5 = x - lo, wrapping per lane
	VUMIN	V1.B16, V5.B16, V6.B16
	VCMEQ	V5.B16, V6.B16, V6.B16  // V6 = lanes where x-lo <= length, unsigned
	VUMAX	V2.B16, V4.B16, V7.B16
	VCMEQ	V4.B16, V7.B16, V7.B16  // V7 = lanes where x >= 2, that is occupied
	VAND	V6.B16, V7.B16, V7.B16
	VMOV	V7.D[0], R6             // lanes of the first group
	VMOV	V7.D[1], R7             // lanes of the second group
	CBNZ	R6, done
	CBZ	R7, next16
	ADD	$1, R5
	B	done

next16:
	ADD	$16, R0
	ADD	$2, R5
	SUB	$2, R1
	B	loop16

	// An odd number of groups leaves a single 8 byte group to check.
tail:
	CBZ	R1, notfound
	VLD1	(R0), [V4.B8]
	VSUB	V0.B8, V4.B8, V5.B8
	VUMIN	V1.B8, V5.B8, V6.B8
	VCMEQ	V5.B8, V6.B8, V6.B8
	VUMAX	V2.B8, V4.B8, V7.B8
	VCMEQ	V4.B8, V7.B8, V7.B8
	VAND	V6.B8, V7.B8, V7.B8
	VMOV	V7.D[0], R6
	CBNZ	R6, done

notfound:
	MOVD	n+8(FP), R5

done:
	MOVD	R5, ret+24(FP)
	RET
