package limit

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/limit/functions"
	"github.com/platinummonkey/go-concurrency-limits/measurements"
)

// VegasLimit implements a Limiter based on TCP Vegas where the limit increases by alpha if the queue_use is
// small < alpha and decreases by alpha if the queue_use is large > beta.
//
// Queue size is calculated using the formula,
//   queue_use = limit − BWE×RTTnoLoad = limit × (1 − RTTnoLoad/RTTactual)
//
// For traditional TCP Vegas alpha is typically 2-3 and beta is typically 4-6.  To allow for better growth and stability
// at higher limits we set alpha=Max(3, 10% of the current limit) and beta=Max(6, 20% of the current limit).
type VegasLimit struct {
	estimatedLimit    float64
	maxLimit          int
	rttNoLoad         core.MeasurementInterface
	smoothing         float64
	alphaFunc         func(estimatedLimit int) int
	betaFunc          func(estimatedLimit int) int
	thresholdFunc     func(estimatedLimit int) int
	increaseFunc      func(estimatedLimit float64) float64
	decreaseFunc      func(estimatedLimit float64) float64
	rttSampleListener core.MetricSampleListener
	commonSampler     *core.CommonMetricSampler
	probeMultipler    int
	probeJitter       float64
	probeCount        int64

	listeners []core.LimitChangeListener
	registry  core.MetricRegistry
	logger    Logger
	mu        sync.RWMutex
}

// NewDefaultVegasLimit returns a new default VegasLimit.
func NewDefaultVegasLimit(
	name string,
	logger Logger,
	registry core.MetricRegistry,
	tags ...string,
) *VegasLimit {
	return NewVegasLimitWithRegistry(
		name,
		-1,
		nil,
		-1,
		-1,
		nil,
		nil,
		nil,
		nil,
		nil,
		-1,
		logger,
		registry,
		tags...,
	)
}

// NewDefaultVegasLimitWithLimit creates a new VegasLimit.
func NewDefaultVegasLimitWithLimit(
	name string,
	initialLimit int,
	logger Logger,
	registry core.MetricRegistry,
	tags ...string,
) *VegasLimit {
	return NewVegasLimitWithRegistry(
		name,
		initialLimit,
		nil,
		-1,
		-1,
		nil,
		nil,
		nil,
		nil,
		nil,
		-1,
		logger,
		registry,
		tags...,
	)
}

// NewVegasLimitWithRegistry will create a new VegasLimit.
func NewVegasLimitWithRegistry(
	name string,
	initialLimit int,
	rttNoLoad core.MeasurementInterface,
	maxConcurrency int,
	smoothing float64,
	alphaFunc func(estimatedLimit int) int,
	betaFunc func(estimatedLimit int) int,
	thresholdFunc func(estimatedLimit int) int,
	increaseFunc func(estimatedLimit float64) float64,
	decreaseFunc func(estimatedLimit float64) float64,
	probeMultiplier int,
	logger Logger,
	registry core.MetricRegistry,
	tags ...string,
) *VegasLimit {
	if initialLimit < 1 {
		initialLimit = 20
	}

	if rttNoLoad == nil {
		rttNoLoad = &measurements.MinimumMeasurement{}
	}

	if maxConcurrency < 0 {
		maxConcurrency = 1000
	}

	if smoothing < 0 || smoothing > 1.0 {
		smoothing = 1.0
	}

	if probeMultiplier <= 0 {
		probeMultiplier = 30
	}

	defaultLogFunc := functions.Log10RootFunction(0)
	if alphaFunc == nil {
		alphaFunc = func(limit int) int {
			return 3 * defaultLogFunc(limit)
		}
	}
	if betaFunc == nil {
		betaFunc = func(limit int) int {
			return 6 * defaultLogFunc(limit)
		}
	}
	if thresholdFunc == nil {
		thresholdFunc = func(limit int) int {
			return defaultLogFunc(limit)
		}
	}
	defaultLogFloatFunc := functions.Log10RootFloatFunction(0)
	if increaseFunc == nil {
		increaseFunc = func(limit float64) float64 {
			return limit + defaultLogFloatFunc(limit)
		}
	}
	if decreaseFunc == nil {
		decreaseFunc = func(limit float64) float64 {
			return limit - defaultLogFloatFunc(limit)
		}
	}

	if logger == nil {
		logger = NoopLimitLogger{}
	}

	if registry == nil {
		registry = core.EmptyMetricRegistryInstance
	}

	l := &VegasLimit{
		estimatedLimit:    float64(initialLimit),
		maxLimit:          maxConcurrency,
		alphaFunc:         alphaFunc,
		betaFunc:          betaFunc,
		thresholdFunc:     thresholdFunc,
		increaseFunc:      increaseFunc,
		decreaseFunc:      decreaseFunc,
		smoothing:         smoothing,
		probeMultipler:    probeMultiplier,
		probeJitter:       newProbeJitter(),
		probeCount:        0,
		rttNoLoad:         rttNoLoad,
		rttSampleListener: registry.RegisterDistribution(core.PrefixMetricWithName(core.MetricMinRTT, name), tags...),
		listeners:         make([]core.LimitChangeListener, 0),
		registry:          registry,
		logger:            logger,
	}

	l.commonSampler = core.NewCommonMetricSampler(registry, l, name, tags...)
	return l
}

// ProbeDisabled represents the disabled value for probing.
const ProbeDisabled = -1

func newProbeJitter() float64 {
	return (rand.Float64() / 2.0) + 0.5
}

// EstimatedLimit returns the current estimated limit.
func (l *VegasLimit) EstimatedLimit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int(l.estimatedLimit)
}

// NotifyOnChange will register a callback to receive notification whenever the limit is updated to a new value.
func (l *VegasLimit) NotifyOnChange(consumer core.LimitChangeListener) {
	l.mu.Lock()
	l.listeners = append(l.listeners, consumer)
	l.mu.Unlock()
}

// notifyListeners will call the callbacks on limit changes
func (l *VegasLimit) notifyListeners(newLimit float64) {
	for _, listener := range l.listeners {
		listener(int(newLimit))
	}
}

// OnSample the concurrency limit using a new rtt sample.
func (l *VegasLimit) OnSample(startTime int64, rtt int64, inFlight int, didDrop bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commonSampler.Sample(rtt, inFlight, didDrop)

	l.probeCount++
	if l.shouldProbe() {
		l.logger.Debugf("Probe triggered update to RTT No Load %d ms from %d ms",
			rtt/1e6, int64(l.rttNoLoad.Get())/1e6)
		l.probeJitter = newProbeJitter()
		l.probeCount = 0
		l.rttNoLoad = &measurements.MinimumMeasurement{}
		l.rttNoLoad.Add(float64(rtt))
		return
	}

	if l.rttNoLoad.Get() == 0 || float64(rtt) < l.rttNoLoad.Get() {
		l.logger.Debugf("Update RTT No Load to %d ms from %d ms", rtt/1e6, int64(l.rttNoLoad.Get())/1e6)
		l.rttNoLoad.Add(float64(rtt))
		return
	}

	l.rttSampleListener.AddSample(l.rttNoLoad.Get())
	l.updateEstimatedLimit(startTime, rtt, inFlight, didDrop)
}

func (l *VegasLimit) shouldProbe() bool {
	return int64(l.probeJitter*float64(l.probeMultipler)*l.estimatedLimit) <= l.probeCount
}

func (l *VegasLimit) updateEstimatedLimit(startTime int64, rtt int64, inFlight int, didDrop bool) {
	queueSize := int(math.Ceil(l.estimatedLimit * (1 - l.rttNoLoad.Get()/float64(rtt))))

	var newLimit float64
	// Treat any drop (i.e timeout) as needing to reduce the limit
	if didDrop {
		newLimit = l.decreaseFunc(l.estimatedLimit)
	} else if float64(inFlight)*2 < l.estimatedLimit {
		// Prevent upward drift if not close to the limit
		return
	} else {
		alpha := l.alphaFunc(int(l.estimatedLimit))
		beta := l.betaFunc(int(l.estimatedLimit))
		threshold := l.thresholdFunc(int(l.estimatedLimit))

		if queueSize < threshold {
			// Aggressive increase when no queuing
			newLimit = l.estimatedLimit + float64(beta)
		} else if queueSize < alpha {
			// Increase the limit if queue is still manageable
			newLimit = l.increaseFunc(l.estimatedLimit)
		} else if queueSize > beta {
			// Detecting latency so decrease
			newLimit = l.decreaseFunc(l.estimatedLimit)
		} else {
			// otherwise we're within he sweet spot so nothing to do
			return
		}
	}

	newLimit = math.Max(1, math.Min(float64(l.maxLimit), newLimit))
	newLimit = (1-l.smoothing)*l.estimatedLimit + l.smoothing*newLimit

	if int(newLimit) != int(l.estimatedLimit) && l.logger.IsDebugEnabled() {
		l.logger.Debugf("New limit=%d, minRTT=%d ms, winRTT=%d ms, queueSize=%d",
			int(newLimit), int64(l.rttNoLoad.Get())/1e6, rtt/1e6, queueSize)
	}

	l.estimatedLimit = newLimit
	l.notifyListeners(l.estimatedLimit)
}

// RTTNoLoad returns the current RTT No Load value.
func (l *VegasLimit) RTTNoLoad() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(l.rttNoLoad.Get())
}

func (l *VegasLimit) String() string {
	return fmt.Sprintf("VegasLimit{limit=%d, rttNoLoad=%d ms}",
		l.EstimatedLimit(), l.RTTNoLoad())
}
