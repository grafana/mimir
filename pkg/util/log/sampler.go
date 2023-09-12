// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"
)

type SampledError struct {
	err     error
	sampler *Sampler
}

func (s SampledError) Error() string {
	if s.sampler == nil {
		return s.err.Error()
	}
	return fmt.Sprintf("%s (sampled 1/%d)", s.err.Error(), s.sampler.freq)
}

func (s SampledError) Unwrap() error { return s.err }

// ShouldLog is called by common logging module.
func (s SampledError) ShouldLog(_ context.Context, _ time.Duration) bool {
	return s.sampler == nil || s.sampler.Sample()
}

type Sampler struct {
	freq  int64
	count atomic.Int64
}

func NewSampler(freq int64) *Sampler {
	if freq == 0 {
		return nil
	}
	return &Sampler{freq: freq}
}

func (s *Sampler) Sample() bool {
	count := s.count.Inc()
	return (count-1)%s.freq == 0
}

func (s *Sampler) WrapError(err error) error {
	if s == nil {
		return err
	}
	return SampledError{err: err, sampler: s}
}
