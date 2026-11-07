// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResponseCollector_DrainOnCancel(t *testing.T) {
	f := func(s string) Frame { return Frame(s) }

	tests := map[string]struct {
		primary   []Frame
		secondary []Frame
		expected  []ResponseFramePair
	}{
		"no frames": {},
		"equal number of frames are paired in order": {
			primary:   []Frame{f("p1"), f("p2")},
			secondary: []Frame{f("s1"), f("s2")},
			expected: []ResponseFramePair{
				{primary: f("p1"), secondary: f("s1")},
				{primary: f("p2"), secondary: f("s2")},
			},
		},
		"extra primary frames are sent unpaired": {
			primary:   []Frame{f("p1"), f("p2"), f("p3")},
			secondary: []Frame{f("s1")},
			expected: []ResponseFramePair{
				{primary: f("p1"), secondary: f("s1")},
				{primary: f("p2")},
				{primary: f("p3")},
			},
		},
		"extra secondary frames are sent unpaired": {
			primary:   []Frame{f("p1")},
			secondary: []Frame{f("s1"), f("s2"), f("s3")},
			expected: []ResponseFramePair{
				{primary: f("p1"), secondary: f("s1")},
				{secondary: f("s2")},
				{secondary: f("s3")},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rc := NewResponseCollector()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- rc.RunCollect(ctx) }()

			for _, frame := range tc.primary {
				rc.PushPrimary(frame)
			}
			for _, frame := range tc.secondary {
				rc.PushSecondary(frame)
			}
			cancel()

			select {
			case err := <-errCh:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("RunCollect did not return after cancel")
			}

			// RunCollect has returned, so every pair and the end marker are in the send queue.
			var actual []ResponseFramePair
			for {
				pair, ok := rc.PopSync()
				if !ok {
					break
				}
				actual = append(actual, pair)
			}
			require.Equal(t, tc.expected, actual)

			// Later calls must also report the end.
			_, ok := rc.PopSync()
			require.False(t, ok)
		})
	}
}

func TestResponseCollector_PairsWhileRunning(t *testing.T) {
	f := func(s string) Frame { return Frame(s) }

	rc := NewResponseCollector()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- rc.RunCollect(ctx) }()

	// Push both primary frames first, so the second one waits for its partner.
	rc.PushPrimary(f("p1"))
	rc.PushPrimary(f("p2"))
	rc.PushSecondary(f("s1"))
	rc.PushSecondary(f("s2"))

	// The pairs must arrive before the cancel, so the loop made them, not the drain.
	pair, ok := popWithTimeout(t, rc)
	require.True(t, ok)
	require.Equal(t, ResponseFramePair{primary: f("p1"), secondary: f("s1")}, pair)

	pair, ok = popWithTimeout(t, rc)
	require.True(t, ok)
	require.Equal(t, ResponseFramePair{primary: f("p2"), secondary: f("s2")}, pair)

	// A frame with no partner is sent unpaired when the context is canceled.
	rc.PushPrimary(f("p3"))
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("RunCollect did not return after cancel")
	}

	pair, ok = popWithTimeout(t, rc)
	require.True(t, ok)
	require.Equal(t, ResponseFramePair{primary: f("p3")}, pair)

	_, ok = popWithTimeout(t, rc)
	require.False(t, ok)
}

// popWithTimeout calls PopSync and fails the test if it blocks for too long.
func popWithTimeout(t *testing.T, rc *ResponseCollector) (ResponseFramePair, bool) {
	t.Helper()

	type result struct {
		pair ResponseFramePair
		ok   bool
	}
	resCh := make(chan result, 1)
	go func() {
		pair, ok := rc.PopSync()
		resCh <- result{pair: pair, ok: ok}
	}()

	select {
	case res := <-resCh:
		return res.pair, res.ok
	case <-time.After(1 * time.Second):
		t.Fatal("PopSync blocked for too long")
		return ResponseFramePair{}, false
	}
}
