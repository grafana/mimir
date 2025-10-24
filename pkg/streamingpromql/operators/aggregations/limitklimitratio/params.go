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

var _ param = (*ratioParam)(nil)
var _ param = (*kParam)(nil)

type param interface {
	quietCloser
	allZero() bool
}

// ratioParam is a utility to assisting in the parsing and management of the ratio parameter used in each step of a query
type ratioParam struct {
	r              []float64 // The ratio value for each step
	rStepInvariant bool      // True if r is constant across all steps

	annotations                     *annotations.Annotations
	haveEmittedRatioAboveAnnotation bool
	haveEmittedRatioBelowAnnotation bool
	memoryConsumptionTracker        *limiter.MemoryConsumptionTracker
	stepCount                       int
	param                           types.ScalarOperator
}

func newRatioParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (*ratioParam, error) {
	r := &ratioParam{
		rStepInvariant:           true, // this is updated in init()
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

func (p *ratioParam) allZero() bool {
	return p.r[0] == float64(0) && p.rStepInvariant
}

func (p *ratioParam) init(ctx context.Context) error {
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

	for stepIdx := 0; stepIdx < p.stepCount; stepIdx++ {
		v := paramValues.Samples[stepIdx].F

		if err := p.validateLimitRatioParam(v); err != nil {
			return err
		}

		if v < -1 {
			v = float64(-1)
		}

		if v > 1 {
			v = float64(1)
		}

		p.r[stepIdx] = v

		if stepIdx > 0 && p.r[stepIdx-1] != p.r[stepIdx] {
			p.rStepInvariant = false
		}
	}

	return nil
}

func (p *ratioParam) close() {
	types.Float64SlicePool.Put(&p.r, p.memoryConsumptionTracker)
}

func (p *ratioParam) validateLimitRatioParam(v float64) error {

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

// kParam is a utility to assisting in the parsing and management of the k parameter used in each step of a query
type kParam struct {
	k              []int64 // The k value for each step - only used when ratio==false
	kMax           int64   // The max(k) across all steps
	kStepInvariant bool    // True if r is constant across all steps

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	stepCount                int
	param                    types.ScalarOperator
}

func newKParam(ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (*kParam, error) {
	r := &kParam{
		kStepInvariant:           true, // this is updated in init()
		memoryConsumptionTracker: memoryConsumptionTracker,
		stepCount:                stepCount,
		param:                    param,
	}

	if err := r.init(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (p *kParam) allZero() bool {
	return p.k[0] == int64(0) && p.kStepInvariant
}

func (p *kParam) init(ctx context.Context) error {
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

	p.kMax = int64(0)

	for stepIdx := 0; stepIdx < p.stepCount; stepIdx++ {
		v := paramValues.Samples[stepIdx].F

		if err := p.validateLimitKParam(v); err != nil {
			return err
		}

		p.k[stepIdx] = max(int64(v), 0) // Ignore any negative values.

		if p.k[stepIdx] > 0 {
			p.kMax = max(p.kMax, p.k[stepIdx])
		}
	}

	return nil
}

func (p *kParam) close() {
	types.Int64SlicePool.Put(&p.k, p.memoryConsumptionTracker)
}

func (p *kParam) validateLimitKParam(v float64) error {
	// Note that these error strings need to match the prometheus engine.
	//nolint:staticcheck
	if math.IsNaN(v) {
		return errors.New("Parameter value is NaN")
	}
	//nolint:staticcheck
	if v <= math.MinInt64 {
		return fmt.Errorf("Scalar value %v underflows int64", v)
	}
	//nolint:staticcheck
	if v >= math.MaxInt64 {
		return fmt.Errorf("Scalar value %v overflows int64", v)
	}

	return nil
}
