package bench

import (
	"bytes"
	"context"
	"fmt"
	"hash/adler32"
	"math/rand"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/prompb"
)

type SeriesType string

const (
	GaugeZero     SeriesType = "gauge-zero"
	GaugeRandom   SeriesType = "gauge-random"
	CounterOne    SeriesType = "counter-one"
	CounterRandom SeriesType = "counter-random"
)

type LabelDesc struct {
	Name         string `yaml:"name"`
	ValuePrefix  string `yaml:"value_prefix"`
	UniqueValues int    `yaml:"unique_values"`
}

type SeriesDesc struct {
	Name         string            `yaml:"name"`
	Type         SeriesType        `yaml:"type"`
	StaticLabels map[string]string `yaml:"static_labels"`
	Labels       []LabelDesc       `yaml:"labels"`
}

type QueryDesc struct {
	NumQueries               int           `yaml:"num_queries"`
	ExprTemplate             string        `yaml:"expr_template"`
	RequiredSeriesType       SeriesType    `yaml:"series_type"`
	Interval                 time.Duration `yaml:"interval"`
	TimeRange                time.Duration `yaml:"time_range,omitempty"`
	Regex                    bool          `yaml:"regex"`
	InjectExactSerierMatcher bool          `yaml:"inject_exact_series_matcher"`
}

type WriteDesc struct {
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
	BatchSize int           `yaml:"batch_size"`
}

type WorkloadDesc struct {
	Replicas  int          `yaml:"replicas"`
	Series    []SeriesDesc `yaml:"series"`
	QueryDesc []QueryDesc  `yaml:"queries"`
	Write     WriteDesc    `yaml:"write_options"`
}

type timeseries struct {
	labelSets  [][]prompb.Label
	lastValue  float64
	seriesType SeriesType
}

type writeWorkload struct {
	replicas           int
	series             []*timeseries
	totalSeries        int
	totalSeriesTypeMap map[SeriesType]int

	missedIterations prometheus.Counter

	options WriteDesc

	seriesBufferChan chan []prompb.TimeSeries
}

func newWriteWorkload(workloadDesc WorkloadDesc, reg prometheus.Registerer) *writeWorkload {
	totalSeries := 0
	totalSeriesTypeMap := map[SeriesType]int{
		GaugeZero:     0,
		GaugeRandom:   0,
		CounterOne:    0,
		CounterRandom: 0,
	}

	series := []*timeseries{}

	for _, seriesDesc := range workloadDesc.Series {
		// Create the metric with a name value
		labelSets := [][]prompb.Label{
			{
				prompb.Label{Name: "__name__", Value: seriesDesc.Name},
			},
		}

		// Add any configured static labels
		for labelName, labelValue := range seriesDesc.StaticLabels {
			labelSets[0] = append(labelSets[0], prompb.Label{Name: labelName, Value: labelValue})
		}

		// Create the dynamic label set
		for _, lbl := range seriesDesc.Labels {
			labelSets = addLabelToLabelSet(labelSets, lbl)
		}

		series = append(series, &timeseries{
			labelSets:  labelSets,
			seriesType: seriesDesc.Type,
		})
		numSeries := len(labelSets)
		totalSeries += numSeries
		totalSeriesTypeMap[seriesDesc.Type] += numSeries
	}

	// Set batch size to 500 samples if not set
	if workloadDesc.Write.BatchSize == 0 {
		workloadDesc.Write.BatchSize = 500
	}

	// Set the write interval to 15 seconds if not set
	if workloadDesc.Write.Interval == 0 {
		workloadDesc.Write.Interval = time.Second * 15
	}

	// Set the write timeout to 15 seconds if not set
	if workloadDesc.Write.Timeout == 0 {
		workloadDesc.Write.Timeout = time.Second * 15
	}

	return &writeWorkload{
		replicas:           workloadDesc.Replicas,
		series:             series,
		totalSeries:        totalSeries,
		totalSeriesTypeMap: totalSeriesTypeMap,
		options:            workloadDesc.Write,

		missedIterations: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "benchtool",
				Name:      "write_iterations_late_total",
				Help:      "Number of write intervals started late because the previous interval did not complete in time.",
			},
		),
	}
}

func addLabelToLabelSet(labelSets [][]prompb.Label, lbl LabelDesc) [][]prompb.Label {
	newLabelSets := make([][]prompb.Label, 0, len(labelSets)*lbl.UniqueValues)
	for i := 0; i < lbl.UniqueValues; i++ {
		for _, labelSet := range labelSets {
			newSet := make([]prompb.Label, len(labelSet)+1)
			copy(newSet, labelSet)

			newSet[len(newSet)-1] = prompb.Label{
				Name:  lbl.Name,
				Value: fmt.Sprintf("%s-%v", lbl.ValuePrefix, i),
			}
			newLabelSets = append(newLabelSets, newSet)
		}
	}
	return newLabelSets
}

func (w *writeWorkload) generateTimeSeries(id string, t time.Time) []prompb.TimeSeries {
	now := t.UnixNano() / int64(time.Millisecond)

	timeseries := make([]prompb.TimeSeries, 0, w.replicas*w.totalSeries)
	for replicaNum := 0; replicaNum < w.replicas; replicaNum++ {
		replicaLabel := prompb.Label{Name: "bench_replica", Value: fmt.Sprintf("replica-%05d", replicaNum)}
		idLabel := prompb.Label{Name: "bench_id", Value: id}
		for _, series := range w.series {
			var value float64
			switch series.seriesType {
			case GaugeZero:
				value = 0
			case GaugeRandom:
				value = rand.Float64()
			case CounterOne:
				value = series.lastValue + 1
			case CounterRandom:
				value = series.lastValue + float64(rand.Int())
			default:
				panic(fmt.Sprintf("unknown series type %v", series.seriesType))
			}
			series.lastValue = value
			for _, labelSet := range series.labelSets {
				newLabelSet := make([]prompb.Label, len(labelSet)+2)
				copy(newLabelSet, labelSet)

				newLabelSet[len(newLabelSet)-2] = replicaLabel
				newLabelSet[len(newLabelSet)-1] = idLabel
				timeseries = append(timeseries, prompb.TimeSeries{
					Labels: newLabelSet,
					Samples: []prompb.Sample{{
						Timestamp: now,
						Value:     value,
					}},
				})
			}
		}
	}

	return timeseries
}

type batchReq struct {
	batch   []prompb.TimeSeries
	wg      *sync.WaitGroup
	putBack chan []prompb.TimeSeries
}

func (w *writeWorkload) getSeriesBuffer(ctx context.Context) []prompb.TimeSeries {
	select {
	case <-ctx.Done():
		return nil
	case seriesBuffer := <-w.seriesBufferChan:
		return seriesBuffer[:0]
	}
}

func (w *writeWorkload) generateWriteBatch(ctx context.Context, id string, numBuffers int, seriesChan chan batchReq) error {
	w.seriesBufferChan = make(chan []prompb.TimeSeries, numBuffers)
	for i := 0; i < numBuffers; i++ {
		w.seriesBufferChan <- make([]prompb.TimeSeries, 0, w.options.BatchSize)
	}

	seriesBuffer := w.getSeriesBuffer(ctx)
	ticker := time.NewTicker(w.options.Interval)

	defer close(seriesChan)

	for {
		select {
		case <-ctx.Done():
			return nil
		case timeNow := <-ticker.C:
			now := timeNow.UnixNano() / int64(time.Millisecond)
			wg := &sync.WaitGroup{}
			for replicaNum := 0; replicaNum < w.replicas; replicaNum++ {
				replicaLabel := prompb.Label{Name: "bench_replica", Value: fmt.Sprintf("replica-%05d", replicaNum)}
				idLabel := prompb.Label{Name: "bench_id", Value: id}
				for _, series := range w.series {
					var value float64
					switch series.seriesType {
					case GaugeZero:
						value = 0
					case GaugeRandom:
						value = rand.Float64()
					case CounterOne:
						value = series.lastValue + 1
					case CounterRandom:
						value = series.lastValue + float64(rand.Int())
					default:
						return fmt.Errorf("unknown series type %v", series.seriesType)
					}
					series.lastValue = value
					for _, labelSet := range series.labelSets {
						if len(seriesBuffer) == w.options.BatchSize {
							wg.Add(1)
							seriesChan <- batchReq{seriesBuffer, wg, w.seriesBufferChan}
							seriesBuffer = w.getSeriesBuffer(ctx)
						}
						newLabelSet := make([]prompb.Label, len(labelSet)+2)
						copy(newLabelSet, labelSet)

						newLabelSet[len(newLabelSet)-2] = replicaLabel
						newLabelSet[len(newLabelSet)-1] = idLabel
						seriesBuffer = append(seriesBuffer, prompb.TimeSeries{
							Labels: newLabelSet,
							Samples: []prompb.Sample{{
								Timestamp: now,
								Value:     value,
							}},
						})
					}
				}
			}
			if len(seriesBuffer) > 0 {
				wg.Add(1)
				seriesChan <- batchReq{seriesBuffer, wg, w.seriesBufferChan}
				seriesBuffer = w.getSeriesBuffer(ctx)
			}
			wg.Wait()
			if time.Since(timeNow) > w.options.Interval {
				w.missedIterations.Inc()
			}
		}
	}
}

type queryWorkload struct {
	queries []query
}

type exprTemplateData struct {
	Name     string
	Matchers string
}

type query struct {
	interval  time.Duration
	timeRange time.Duration
	expr      string
}

func newQueryWorkload(id string, desc WorkloadDesc) (*queryWorkload, error) {
	seriesTypeMap := map[SeriesType][]SeriesDesc{
		GaugeZero:     nil,
		GaugeRandom:   nil,
		CounterOne:    nil,
		CounterRandom: nil,
	}

	for _, s := range desc.Series {
		seriesSlice, ok := seriesTypeMap[s.Type]
		if !ok {
			return nil, fmt.Errorf("series found with unknown series type %s", s.Type)
		}

		seriesTypeMap[s.Type] = append(seriesSlice, s)
	}

	// Use the provided ID to create a random seed. This will ensure repeated runs with the same
	// configured ID will produce the same query workloads.
	hashSeed := adler32.Checksum([]byte(id))
	rand := rand.New(rand.NewSource(int64(hashSeed)))

	queries := []query{}
	for _, queryDesc := range desc.QueryDesc {
		exprTemplate, err := template.New("query").Delims("<<", ">>").Parse(queryDesc.ExprTemplate)
		if err != nil {
			return nil, fmt.Errorf("unable to parse query template, %v", err)
		}

		for i := 0; i < queryDesc.NumQueries; i++ {
			seriesSlice, ok := seriesTypeMap[queryDesc.RequiredSeriesType]
			if !ok {
				return nil, fmt.Errorf("query found with unknown series type %s", queryDesc.RequiredSeriesType)
			}

			if len(seriesSlice) == 0 {
				return nil, fmt.Errorf("no series found for query with series type %s", queryDesc.RequiredSeriesType)
			}

			seriesDesc := seriesSlice[rand.Intn(len(seriesSlice))]

			matchers := []string{}

			// Select a random bench replica to inject as a matcher
			replicaNum := rand.Intn(desc.Replicas)
			if queryDesc.Regex {
				matchers = append(matchers, "bench_replica=\""+fmt.Sprintf("replica-%05d", replicaNum)+"\"")
			} else {
				matchers = append(matchers, "bench_replica=~\""+fmt.Sprintf("replica-%05d", replicaNum)+"\"")
			}

			var b bytes.Buffer
			err = exprTemplate.Execute(&b, exprTemplateData{
				Name:     seriesDesc.Name,
				Matchers: strings.Join(matchers, ", "),
			})
			if err != nil {
				return nil, fmt.Errorf("unable to execute expr_template %s, %w", queryDesc.ExprTemplate, err)
			}

			queries = append(queries, query{
				interval:  queryDesc.Interval,
				timeRange: queryDesc.TimeRange,
				expr:      b.String(),
			})
		}
	}

	return &queryWorkload{queries}, nil
}
