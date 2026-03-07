package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
)

type Simulation struct {
	cfg     Config
	metrics []MetricInfo
	series  []Series // sorted by hash
	partitions []*Partition
	ingesters  []*Ingester
	catchUps   []CatchUpEntry
	rng        *rand.Rand

	// Recomputed after assignment changes.
	rangeIngestion     map[HashRange]float64
	rangeQuery         map[HashRange]float64
	partitionIngestion []float64 // indexed by partition ID
	partitionQuery     []float64 // indexed by partition ID
	ingesterQuery      []float64 // indexed by ingester ID (adjusted for catch-ups)
}

func NewSimulation(cfg Config) (*Simulation, error) {
	rng := rand.New(rand.NewSource(cfg.Seed))
	metrics, series := generateSyntheticData(cfg, rng)

	sim := &Simulation{
		cfg:                cfg,
		metrics:            metrics,
		series:             series,
		rng:                rng,
		rangeIngestion:     make(map[HashRange]float64),
		rangeQuery:         make(map[HashRange]float64),
		partitionIngestion: make([]float64, cfg.NumPartitions),
		partitionQuery:     make([]float64, cfg.NumPartitions),
		ingesterQuery:      make([]float64, cfg.NumIngesters),
	}
	sim.initPartitions()
	sim.initIngesters()
	return sim, nil
}

func (sim *Simulation) initPartitions() {
	n := sim.cfg.NumPartitions
	sim.partitions = make([]*Partition, n)
	rangeSize := uint64(1<<32) / uint64(n)
	for i := 0; i < n; i++ {
		lo := uint32(uint64(i) * rangeSize)
		var hi uint32
		if i == n-1 {
			hi = 0 // sentinel for 2^32
		} else {
			hi = uint32(uint64(i+1) * rangeSize)
		}
		hr := HashRange{Lo: lo, Hi: hi}
		sim.partitions[i] = &Partition{
			ID:         int32(i),
			HashRanges: map[HashRange]struct{}{hr: {}},
		}
	}
}

func (sim *Simulation) initIngesters() {
	n := sim.cfg.NumIngesters
	sim.ingesters = make([]*Ingester, n)
	for i := 0; i < n; i++ {
		sim.ingesters[i] = &Ingester{
			ID:         i,
			Partitions: make(map[int32]struct{}),
		}
	}
	for _, p := range sim.partitions {
		ingID := int(p.ID) % n
		sim.ingesters[ingID].Partitions[p.ID] = struct{}{}
	}
}

func (sim *Simulation) Run() {
	w := NewCSVWriter(os.Stdout)
	w.WriteHeader()

	progressInterval := sim.cfg.NumSteps / 10
	if progressInterval == 0 {
		progressInterval = 1
	}

	for t := 0; t < sim.cfg.NumSteps; t++ {
		sim.tickCatchUps()
		sim.computeLoads()

		ingStats := computeStats(normalizeByTotal(sim.partitionIngestion))
		qryStats := computeStats(normalizeByTotal(sim.ingesterQuery))
		catchupCount, catchupLoad := sim.catchUpStats()

		var ingMoves int
		var ingChurn float64
		if t > 0 && t%sim.cfg.IngestionRebalanceInterval == 0 {
			ingMoves, ingChurn = sim.runIngestionRebalancer()
		}

		qryMoves := 0
		if t > 0 && t%sim.cfg.QueryRebalanceInterval == 0 {
			qryMoves = sim.runQueryRebalancer()
		}

		w.WriteRow(t, ingStats, qryStats, ingMoves, qryMoves, ingChurn, catchupCount, catchupLoad)

		if t > 0 && t%progressInterval == 0 {
			fmt.Fprintf(os.Stderr, "  step %d/%d\n", t, sim.cfg.NumSteps)
		}
	}
}

func (sim *Simulation) computeLoads() {
	sim.rangeIngestion = make(map[HashRange]float64, len(sim.rangeIngestion))
	sim.rangeQuery = make(map[HashRange]float64, len(sim.rangeQuery))
	for i := range sim.partitionIngestion {
		sim.partitionIngestion[i] = 0
	}
	for i := range sim.partitionQuery {
		sim.partitionQuery[i] = 0
	}

	for _, p := range sim.partitions {
		for hr := range p.HashRanges {
			ss := sim.seriesInRange(hr)
			ingRate := 0.0
			qryLoad := 0.0
			for _, s := range ss {
				ingRate += sim.metrics[s.MetricIndex].IngestionWeight
				qryLoad += sim.metrics[s.MetricIndex].QueryWeight
			}
			sim.rangeIngestion[hr] = ingRate
			sim.rangeQuery[hr] = qryLoad
			sim.partitionIngestion[p.ID] += ingRate
			sim.partitionQuery[p.ID] += qryLoad
		}
	}

	// Per-ingester query load: sum of partitions' query load, adjusted for catch-ups.
	for i := range sim.ingesterQuery {
		sim.ingesterQuery[i] = 0
	}
	for _, ing := range sim.ingesters {
		for pid := range ing.Partitions {
			sim.ingesterQuery[ing.ID] += sim.partitionQuery[pid]
		}
	}
	for _, cu := range sim.catchUps {
		pq := sim.partitionQuery[cu.PartitionID]
		sim.ingesterQuery[cu.NewIngesterID] -= pq
		sim.ingesterQuery[cu.OldIngesterID] += pq
		sim.ingesterQuery[cu.NewIngesterID] += cu.ReplayLoad
	}
}

func (sim *Simulation) seriesInRange(hr HashRange) []Series {
	lo := sort.Search(len(sim.series), func(i int) bool { return sim.series[i].Hash >= hr.Lo })
	if hr.Hi == 0 {
		return sim.series[lo:]
	}
	hi := sort.Search(len(sim.series), func(i int) bool { return sim.series[i].Hash >= hr.Hi })
	return sim.series[lo:hi]
}

func (sim *Simulation) tickCatchUps() {
	active := sim.catchUps[:0]
	for i := range sim.catchUps {
		sim.catchUps[i].RemainingSteps--
		if sim.catchUps[i].RemainingSteps > 0 {
			active = append(active, sim.catchUps[i])
		}
	}
	sim.catchUps = active
}

func (sim *Simulation) partitionInCatchUp(pid int32) bool {
	for _, cu := range sim.catchUps {
		if cu.PartitionID == pid {
			return true
		}
	}
	return false
}

func (sim *Simulation) catchUpStats() (int, float64) {
	totalLoad := 0.0
	for _, cu := range sim.catchUps {
		totalLoad += cu.ReplayLoad
	}
	return len(sim.catchUps), totalLoad
}

func (sim *Simulation) computeRangeLoads(hr HashRange) (ingestion, query float64) {
	ss := sim.seriesInRange(hr)
	for _, s := range ss {
		ingestion += sim.metrics[s.MetricIndex].IngestionWeight
		query += sim.metrics[s.MetricIndex].QueryWeight
	}
	return
}
