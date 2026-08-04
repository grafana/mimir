// SPDX-License-Identifier: AGPL-3.0-only

package common

import (
	"context"
	"io"
	"net/http"
	"time"
)

// HedgingRoundTripper sends up to UpTo copies of slow idempotent requests, spaced Delay
// apart, and returns whichever response arrives first. Later responses are discarded and
// their attempts canceled. This trades a small amount of duplicate load (~1% of requests
// when Delay sits near the p99 latency) for a large reduction in tail latency: a request
// is slow only if every attempt independently draws a slow backend.
//
// Only requests without a body using GET or HEAD are hedged; everything else is passed
// through unchanged, so non-idempotent operations such as uploads are never duplicated.
type HedgingRoundTripper struct {
	// Next is the underlying round tripper hedged requests are sent to.
	Next http.RoundTripper
	// Delay is how long to wait for a response before launching the next attempt.
	Delay time.Duration
	// UpTo is the maximum total number of attempts per request, including the first.
	UpTo int
}

// attemptResult is the outcome of one hedged attempt.
type attemptResult struct {
	idx  int
	resp *http.Response
	err  error
}

func (h *HedgingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if h.Delay <= 0 || h.UpTo < 2 || req.Body != nil || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		return h.Next.RoundTrip(req)
	}

	parent := req.Context()
	// Buffered so attempt goroutines can always deliver their result and exit, even
	// after a winner has been returned.
	results := make(chan attemptResult, h.UpTo)
	cancels := make([]context.CancelFunc, 0, h.UpTo)

	launch := func() {
		ctx, cancel := context.WithCancel(parent)
		cancels = append(cancels, cancel)
		idx := len(cancels) - 1
		attemptReq := req.Clone(ctx)
		go func() {
			resp, err := h.Next.RoundTrip(attemptReq)
			results <- attemptResult{idx: idx, resp: resp, err: err}
		}()
	}

	launch()

	timer := time.NewTimer(h.Delay)
	defer timer.Stop()

	var firstErr error
	inFlight := 1
	for {
		select {
		case <-timer.C:
			if len(cancels) < h.UpTo {
				launch()
				inFlight++
				timer.Reset(h.Delay)
			}
		case res := <-results:
			inFlight--
			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				cancels[res.idx]()
				// All attempts failed and no more can be launched: report the first error.
				if inFlight == 0 && len(cancels) >= h.UpTo {
					return nil, firstErr
				}
				// An attempt failed fast, before the hedge timer fired: launch the next
				// attempt immediately rather than waiting out the remaining delay.
				if inFlight == 0 && len(cancels) < h.UpTo {
					launch()
					inFlight++
					timer.Reset(h.Delay)
				}
				continue
			}
			// Winner: cancel the losers and reap their results in the background so
			// their streams and goroutines are released promptly.
			for i, cancel := range cancels {
				if i != res.idx {
					cancel()
				}
			}
			go reapAttempts(results, inFlight)
			// The winning attempt's context must stay alive until the caller has
			// consumed the body, so its cancel is deferred to Body.Close.
			res.resp.Body = &cancelOnCloseBody{ReadCloser: res.resp.Body, cancel: cancels[res.idx]}
			return res.resp, nil
		case <-parent.Done():
			for _, cancel := range cancels {
				cancel()
			}
			go reapAttempts(results, inFlight)
			return nil, parent.Err()
		}
	}
}

// reapAttempts drains the remaining n attempt results, closing any response bodies.
func reapAttempts(results <-chan attemptResult, n int) {
	for ; n > 0; n-- {
		if res := <-results; res.resp != nil {
			_, _ = io.Copy(io.Discard, res.resp.Body)
			_ = res.resp.Body.Close()
		}
	}
}

// cancelOnCloseBody cancels the winning attempt's context once the response body is
// closed, releasing the attempt's resources without truncating the body read.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
