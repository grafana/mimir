// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitklimitratio

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type stepArgument interface {
	close()
	allZero() bool
}

// stepArgumentRatio is a utility to assisting in the parsing and management of the ratio parameter used in each step of a limit_ratio aggregation
type stepArgumentRatio struct {
	r        []float64 // The ratio value for each step
	rAllZero bool      // True if all the r values are set to 0

	annotations                     *annotations.Annotations
	haveEmittedRatioAboveAnnotation bool
	haveEmittedRatioBelowAnnotation bool
	memoryConsumptionTracker        *limiter.MemoryConsumptionTracker
	stepCount                       int
	param                           types.ScalarOperator
}

func newStepArgumentRatio(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (*stepArgumentRatio, error) {
	r := &stepArgumentRatio{
		annotations:              annotations,
		memoryConsumptionTracker: memoryConsumptionTracker,
		stepCount:                stepCount,
		param:                    param,
	}

	if err := r.init(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (p *stepArgumentRatio) allZero() bool {
	return p.rAllZero
}

func (p *stepArgumentRatio) init(ctx context.Context) error {
	// note that k can change per step. ie count(limit_ratio(scalar(foo), http_requests))
	paramValues, err := p.param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(&paramValues.Samples, p.memoryConsumptionTracker)

	// these will be values in the range of -1,1
	p.r, err = types.Float64SlicePool.Get(p.stepCount, p.memoryConsumptionTracker)
	if err != nil {
		return err
	}
	p.r = p.r[:p.stepCount]
	p.rAllZero = true

	for stepIdx := 0; stepIdx < p.stepCount; stepIdx++ {
		v := paramValues.Samples[stepIdx].F

		if err := p.validateLimitRatioParam(v); err != nil {
			return err
		}

		if v < -1 {
			v = float64(-1)

		} else if v > 1 {
			v = float64(1)
		}

		if v != float64(0) {
			p.rAllZero = false
		}

		p.r[stepIdx] = v

	}

	return nil
}

func (p *stepArgumentRatio) close() {
	types.Float64SlicePool.Put(&p.r, p.memoryConsumptionTracker)
}

func (p *stepArgumentRatio) validateLimitRatioParam(v float64) error {

	if math.IsNaN(v) {
		// Note that this error string needs to match the prometheus engine
		//nolint:staticcheck
		return fmt.Errorf("Ratio value is NaN")
	}

	if v > 1.0 {
		if !p.haveEmittedRatioAboveAnnotation {
			p.annotations.Add(annotations.NewInvalidRatioWarning(v, 1.0, p.param.ExpressionPosition()))
			p.haveEmittedRatioAboveAnnotation = true
		}
	}

	if v < -1.0 {
		if !p.haveEmittedRatioBelowAnnotation {
			p.annotations.Add(annotations.NewInvalidRatioWarning(v, -1.0, p.param.ExpressionPosition()))
			p.haveEmittedRatioBelowAnnotation = true
		}
	}

	return nil
}

// stepArgumentK is a utility to assisting in the parsing and management of the k parameter used in each step of a limitk aggregation
type stepArgumentK struct {
	k        []int64 // The k value for each step - only used when ratio==false
	kMax     int64   // The max(k) across all steps
	kAllZero bool    // True if k is zero across all steps

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	stepCount                int
	param                    types.ScalarOperator
}

func newStepArgumentK(ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (*stepArgumentK, error) {
	r := &stepArgumentK{
		memoryConsumptionTracker: memoryConsumptionTracker,
		stepCount:                stepCount,
		param:                    param,
	}

	if err := r.init(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (p *stepArgumentK) allZero() bool {
	return p.kAllZero
}

func (p *stepArgumentK) init(ctx context.Context) error {
	// note that k can change per step. ie count(limitk(scalar(foo), http_requests))
	paramValues, err := p.param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(&paramValues.Samples, p.memoryConsumptionTracker)

	p.k, err = types.Int64SlicePool.Get(p.stepCount, p.memoryConsumptionTracker)
	if err != nil {
		return err
	}
	p.k = p.k[:p.stepCount]
	p.kAllZero = true
	p.kMax = int64(0)

	for stepIdx := 0; stepIdx < p.stepCount; stepIdx++ {
		v := paramValues.Samples[stepIdx].F

		if err := p.validateLimitKParam(v); err != nil {
			return err
		}

		p.k[stepIdx] = max(int64(v), 0) // Ignore any negative values.

		if p.k[stepIdx] > 0 {
			p.kMax = max(p.kMax, p.k[stepIdx])
			p.kAllZero = false
		}
	}

	return nil
}

func (p *stepArgumentK) close() {
	types.Int64SlicePool.Put(&p.k, p.memoryConsumptionTracker)
}

func (p *stepArgumentK) validateLimitKParam(v float64) error {
	// Note that these error strings need to match the prometheus engine.
	if math.IsNaN(v) {
		//nolint:staticcheck
		return errors.New("Parameter value is NaN")
	}
	if v <= math.MinInt64 {
		//nolint:staticcheck
		return fmt.Errorf("Scalar value %v underflows int64", v)
	}
	if v >= math.MaxInt64 {
		//nolint:staticcheck
		return fmt.Errorf("Scalar value %v overflows int64", v)
	}

	return nil
}
