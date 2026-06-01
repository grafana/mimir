// SPDX-License-Identifier: AGPL-3.0-only

package syntax

import (
	"testing"
	"time"
)

func TestParseExpr(t *testing.T) {
	// The expanded "$__auto" sentinel used by dashboard-linter.
	const autoDuration = 12345 * time.Millisecond

	for _, tc := range []struct {
		name         string
		expr         string
		wantLogRange bool
		wantInterval time.Duration
	}{
		{
			name:         "no range selector",
			expr:         `sum(count_over_time({app="foo"} |= "bar"))`,
			wantLogRange: false,
		},
		{
			name:         "fixed duration range",
			expr:         `rate({app="foo"}[5m])`,
			wantLogRange: true,
			wantInterval: 5 * time.Minute,
		},
		{
			name:         "expanded $__auto sentinel parses to auto duration",
			expr:         `rate({app="foo"}[12345ms])`,
			wantLogRange: true,
			wantInterval: autoDuration,
		},
		{
			name:         "unparseable duration falls back to zero",
			expr:         `rate({app="foo"}[1d])`,
			wantLogRange: true,
			wantInterval: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := ParseExpr(tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			lr, ok := expr.(*LogRange)
			if ok != tc.wantLogRange {
				t.Fatalf("got LogRange=%v, want %v (expr=%v)", ok, tc.wantLogRange, expr)
			}
			if ok && lr.Interval != tc.wantInterval {
				t.Fatalf("got interval %v, want %v", lr.Interval, tc.wantInterval)
			}
		})
	}
}
