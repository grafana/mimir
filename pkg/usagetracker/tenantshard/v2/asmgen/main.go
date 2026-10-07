// SPDX-License-Identifier: AGPL-3.0-only

// Command asmgen generates map_simd_amd64.s, the AVX2 implementation of the group operations of
// tenantshard/v2.Map. It is run by "go generate" in the parent package.
//
// The Go declarations of the generated functions are in map_simd_amd64.go, and go vet checks that
// they match the generated frame layout. The signatures below use basic types with the same names
// and sizes, so that avo does not need to load the v2 package, which only builds for amd64.
package main

import (
	. "github.com/mmcloughlin/avo/build" //nolint:revive,staticcheck // avo programs read like assembly listings.
	"github.com/mmcloughlin/avo/gotypes"
	. "github.com/mmcloughlin/avo/operand" //nolint:revive,staticcheck
)

func main() {
	// GOAMD64=v3 guarantees AVX, AVX2 and POPCNT, so no runtime CPU feature check is needed.
	ConstraintExpr("amd64.v3,!nosimd")

	// ones holds 1 in every lane: a byte x is either empty (0) or a spillmark (1) when min(x, 1) == x.
	ones := splat("ones", 1)
	// cycle and maxAhead are the constants of clock.Minutes.GreaterOrEqualThan: the 120 minutes of
	// the clock face, and the largest aheadOf that is still below 60.
	cycle := splat("cycle", 120)
	maxAhead := splat("maxAhead", 59)

	// marks is what Cleanup writes into a removed slot: empty (0) everywhere, except for a spillmark (1)
	// in the last slot, which keeps the signal that the group may have spilled into the next one.
	marks := GLOBL("marks", RODATA|NOPTR)
	DATA(0, U64(0))
	DATA(8, U64(0x0100000000000000))

	match()
	matchEmptyOrSpillmark(ones)
	cleanupGroup(ones, cycle, maxAhead, marks)

	Generate()
}

// splat declares a 16 byte constant that holds b in every lane.
func splat(name string, b uint8) Mem {
	m := GLOBL(name, RODATA|NOPTR)
	lanes := uint64(b) * 0x0101010101010101
	DATA(0, U64(lanes))
	DATA(8, U64(lanes))
	return m
}

func match() {
	TEXT("matchAVX2", NOSPLIT, "func(idx *[16]uint8, p uint8) uint16")

	idx := Mem{Base: Load(Param("idx"), GP64())}
	x := XMM()
	VPBROADCASTB(addr(Param("p")), x)
	VPCMPEQB(idx, x, x)

	mask := GP32()
	VPMOVMSKB(x, mask)
	Store(mask.As16(), ReturnIndex(0))
	RET()
}

func matchEmptyOrSpillmark(ones Mem) {
	TEXT("matchEmptyOrSpillmarkAVX2", NOSPLIT, "func(idx *[16]uint8) uint16")

	x := XMM()
	VMOVDQU(Mem{Base: Load(Param("idx"), GP64())}, x)
	free := XMM()
	VPMINUB(ones, x, free)
	VPCMPEQB(x, free, free)

	mask := GP32()
	VPMOVMSKB(free, mask)
	Store(mask.As16(), ReturnIndex(0))
	RET()
}

func cleanupGroup(ones, cycle, maxAhead, marks Mem) {
	TEXT("cleanupGroupAVX2", NOSPLIT, "func(idx *[16]uint8, d *[16]uint8, watermark uint8) int")

	d := Mem{Base: Load(Param("d"), GP64())}
	x := XMM()
	VMOVDQU(d, x)

	Comment("v = ^x turns the data back into clock.Minutes.")
	v := XMM()
	VPCMPEQB(x, x, v)
	VPXOR(x, v, v)

	Comment("d := watermark - v is negative when watermark < v, that is when max(watermark, v) != watermark.")
	w, ge := XMM(), XMM()
	VPBROADCASTB(addr(Param("watermark")), w)
	VPMAXUB(v, w, ge)
	VPCMPEQB(w, ge, ge)

	Comment(
		"aheadOf = d + (d>>63)&120: add 120 to the lanes where d is negative.",
		"Go computes this in int64 and here it wraps at 256, but aheadOf is always in [-135, 255],",
		"and the wrap only moves [-135, -1] to [121, 255], which is not below 60 either.",
	)
	ahead, fix := XMM(), XMM()
	VPSUBB(v, w, ahead)
	VPANDN(cycle, ge, fix)
	VPADDB(fix, ahead, ahead)

	Comment("aheadOf < 60, that is min(aheadOf, 59) == aheadOf.")
	expired := XMM()
	VPMINUB(maxAhead, ahead, expired)
	VPCMPEQB(ahead, expired, expired)

	Comment("Lanes that hold no data: min(x, 1) == x. Data and index always agree on which slots these are.")
	free := XMM()
	VPMINUB(ones, x, free)
	VPCMPEQB(x, free, free)

	Comment("The lanes to remove are the expired ones that hold data.")
	remove := XMM()
	VPANDN(expired, free, remove)

	Comment("POPCNT sets ZF when nothing is removed, and then neither the index nor the data are written.")
	mask := GP32()
	VPMOVMSKB(remove, mask)
	POPCNTL(mask, mask)
	Store(mask.As64(), ReturnIndex(0))
	JZ(LabelRef("done"))

	Comment("Copy the marks into the removed lanes, first for the data, then for the index.")
	VPBLENDVB(remove, marks, x, x)
	VMOVDQU(x, d)

	idx := Mem{Base: Load(Param("idx"), GP64())}
	i := XMM()
	VMOVDQU(idx, i)
	VPBLENDVB(remove, marks, i, i)
	VMOVDQU(i, idx)

	Label("done")
	RET()
}

// addr returns the stack address of a parameter, so that an instruction can read it from there.
func addr(c gotypes.Component) Mem {
	b, err := c.Resolve()
	if err != nil {
		panic(err)
	}
	return b.Addr
}
