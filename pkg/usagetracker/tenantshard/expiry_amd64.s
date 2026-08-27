// SPDX-License-Identifier: AGPL-3.0-only

//go:build amd64 && !nosimd

#include "textflag.h"

// func scanExpired(p *data, n int, lo, length uint8) int
//
// Evaluates matchExpiredWord over two groups at a time. SSE2 has no unsigned byte compare,
// so "x-lo <= length" is done with a saturating subtract: it lands on zero exactly when the
// lane is within range. The same trick against 1 identifies the empty and tombstone
// markers, which PANDN then removes from the result.
//
// Everything here is SSE2, which is part of the amd64 baseline, so there is no feature
// detection to do.
TEXT ·scanExpired(SB), NOSPLIT, $0-32
	MOVQ	p+0(FP), SI
	MOVQ	n+8(FP), CX
	MOVBLZX	lo+16(FP), AX
	MOVBLZX	length+17(FP), DX

	XORQ	DI, DI                  // index of the group under the cursor
	MOVQ	$0x0101010101010101, R8

	// Broadcast each constant across all 16 lanes by splatting it into a general purpose
	// register first, which avoids the SSE2 unpack dance.
	IMULQ	R8, AX
	MOVQ	AX, X1                  // lo in every lane
	MOVLHPS	X1, X1
	IMULQ	R8, DX
	MOVQ	DX, X2                  // length in every lane
	MOVLHPS	X2, X2
	MOVQ	R8, X3                  // tombstone marker in every lane
	MOVLHPS	X3, X3
	PXOR	X0, X0

loop16:
	CMPQ	CX, $2
	JLT	tail
	MOVOU	(SI), X4
	MOVO	X4, X5
	PSUBB	X1, X5                  // X5 = x - lo, wrapping per lane
	PSUBUSB	X2, X5                  // saturates to zero when x-lo <= length
	PCMPEQB	X0, X5                  // X5 = lanes within range
	MOVO	X4, X6
	PSUBUSB	X3, X6                  // saturates to zero when x <= 1
	PCMPEQB	X0, X6                  // X6 = lanes holding empty or tombstone
	PANDN	X5, X6                  // X6 = in range and occupied
	PMOVMSKB X6, BX
	TESTL	BX, BX
	JNZ	found
	ADDQ	$16, SI
	ADDQ	$2, DI
	SUBQ	$2, CX
	JMP	loop16

found:
	TESTL	$0xff, BX
	JNZ	done                    // the first of the two groups hit
	ADDQ	$1, DI
	JMP	done

	// An odd number of groups leaves a single 8 byte group to check. Loading it into the
	// low half of the register zeroes the high half, and zero lanes read as empty, so they
	// can never produce a spurious hit.
tail:
	TESTQ	CX, CX
	JZ	notfound
	MOVQ	(SI), X4
	MOVO	X4, X5
	PSUBB	X1, X5
	PSUBUSB	X2, X5
	PCMPEQB	X0, X5
	MOVO	X4, X6
	PSUBUSB	X3, X6
	PCMPEQB	X0, X6
	PANDN	X5, X6
	PMOVMSKB X6, BX
	TESTL	$0xff, BX
	JNZ	done

notfound:
	MOVQ	n+8(FP), DI

done:
	MOVQ	DI, ret+24(FP)
	RET
