//go:build validatingpools

package mimirpb

import (
	"fmt"
	"log"
	"os"

	"github.com/grafana/mimir/pkg/util/test"
)

type pool interface {
	Get() any
	Put(any)
}

var (
	preallocTimeseriesSlicePool pool
	timeSeriesPool              pool
	yoloSlicePool               pool
)

func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return preallocTimeseriesSlicePool.Get().([]PreallocTimeseries)
}

func VerifyPools(testingM interface{ Run() int }) {
	fmt.Println("NOTE: Testing with validating pools enabled in mimirpb.")
	t := &testingT{}

	preallocTimeseriesSlicePool = test.NewValidatingPoolForSlice[PreallocTimeseries](t, "preallocTimeseriesSlicePool", false, func() []PreallocTimeseries {
		return make([]PreallocTimeseries, 0, minPreallocatedTimeseries)
	}, nil)
	timeSeriesPool = test.NewValidatingPoolForPointer[*TimeSeries](t, "timeSeriesPool", false, func() *TimeSeries {
		return &TimeSeries{
			Labels:     make([]LabelAdapter, 0, minPreallocatedLabels),
			Samples:    make([]Sample, 0, minPreallocatedSamplesPerSeries),
			Exemplars:  make([]Exemplar, 0, minPreallocatedExemplarsPerSeries),
			Histograms: nil,
		}
	}, nil)
	yoloSlicePool = test.NewValidatingPoolForSlice(t, "yoloSlicePool", false, func() []byte {
		val := make([]byte, 0, 200)
		return val
	}, nil)

	exitCode := testingM.Run()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	if t.failed {
		os.Exit(1)
	}

	os.Exit(0)
}

type testingT struct {
	failed bool
}

func (t *testingT) Errorf(format string, args ...any) {
	t.failed = true
	log.Printf(format, args...)
}

func (t *testingT) Logf(format string, args ...any) {
	log.Printf(format, args...)
}

func (t *testingT) FailNow() { os.Exit(1) }

func (t *testingT) Helper() {}
