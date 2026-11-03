#include "textflag.h"

// Fold the four accumulators V16-V19 into one float and return it.
#define REDUCE \
	VFMLA	V31.S4, V17.S4, V16.S4 \
	VFMLA	V31.S4, V19.S4, V18.S4 \
	VFMLA	V31.S4, V18.S4, V16.S4 \
	VMOV	V16.S[0], R4 \
	FMOVS	R4, F0 \
	VMOV	V16.S[1], R4 \
	FMOVS	R4, F1 \
	VMOV	V16.S[2], R4 \
	FMOVS	R4, F2 \
	VMOV	V16.S[3], R4 \
	FMOVS	R4, F3 \
	FADDS	F1, F0, F0 \
	FADDS	F3, F2, F2 \
	FADDS	F2, F0, F0 \
	FMOVS	F0, ret+24(FP) \
	RET

// func l2NEON(a, b *float32, n int) float32
// Sum of (a[i]-b[i])^2 over n floats; n is a positive multiple of 16.
// Go's assembler has no vector FSUB, so a-b is a fused a - b*1 (exact).
TEXT ·l2NEON(SB), NOSPLIT, $0-28
	MOVD	a+0(FP), R0
	MOVD	b+8(FP), R1
	MOVD	n+16(FP), R2
	MOVW	$0x3f800000, R3
	VDUP	R3, V31.S4
	VEOR	V16.B16, V16.B16, V16.B16
	VEOR	V17.B16, V17.B16, V17.B16
	VEOR	V18.B16, V18.B16, V18.B16
	VEOR	V19.B16, V19.B16, V19.B16
l2loop:
	VLD1.P	64(R0), [V0.S4, V1.S4, V2.S4, V3.S4]
	VLD1.P	64(R1), [V4.S4, V5.S4, V6.S4, V7.S4]
	VFMLS	V31.S4, V4.S4, V0.S4
	VFMLS	V31.S4, V5.S4, V1.S4
	VFMLS	V31.S4, V6.S4, V2.S4
	VFMLS	V31.S4, V7.S4, V3.S4
	VFMLA	V0.S4, V0.S4, V16.S4
	VFMLA	V1.S4, V1.S4, V17.S4
	VFMLA	V2.S4, V2.S4, V18.S4
	VFMLA	V3.S4, V3.S4, V19.S4
	SUBS	$16, R2, R2
	BNE	l2loop
	REDUCE

// func dotNEON(a, b *float32, n int) float32
// Sum of a[i]*b[i] over n floats; n is a positive multiple of 16.
TEXT ·dotNEON(SB), NOSPLIT, $0-28
	MOVD	a+0(FP), R0
	MOVD	b+8(FP), R1
	MOVD	n+16(FP), R2
	MOVW	$0x3f800000, R3
	VDUP	R3, V31.S4
	VEOR	V16.B16, V16.B16, V16.B16
	VEOR	V17.B16, V17.B16, V17.B16
	VEOR	V18.B16, V18.B16, V18.B16
	VEOR	V19.B16, V19.B16, V19.B16
dotloop:
	VLD1.P	64(R0), [V0.S4, V1.S4, V2.S4, V3.S4]
	VLD1.P	64(R1), [V4.S4, V5.S4, V6.S4, V7.S4]
	VFMLA	V0.S4, V4.S4, V16.S4
	VFMLA	V1.S4, V5.S4, V17.S4
	VFMLA	V2.S4, V6.S4, V18.S4
	VFMLA	V3.S4, V7.S4, V19.S4
	SUBS	$16, R2, R2
	BNE	dotloop
	REDUCE
