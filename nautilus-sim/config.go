package main

import "flag"

type Config struct {
	NumPartitions              int
	NumIngesters               int
	NumSteps                   int
	IngestionRebalanceInterval int
	QueryRebalanceInterval     int
	MergeChurnFraction         float64
	MoveChurnFraction          float64
	MinRangesPerPartition      int
	MaxRangesPerPartition      int
	IngestionMoveBudget        int
	QueryMoveBudget            int
	PartitionCatchupSteps      int
	CatchupCostFactor          float64
	BalanceTarget              float64
	NumSyntheticSeries         int
	NumSyntheticMetrics        int
	SkewTopMetricFraction      float64
	MetricHashBits             int
	Seed                       int64
}

func parseConfig() Config {
	var cfg Config
	flag.IntVar(&cfg.NumPartitions, "num-partitions", 385, "Number of Kafka partitions")
	flag.IntVar(&cfg.NumIngesters, "num-ingesters", 200, "Number of ingesters")
	flag.IntVar(&cfg.NumSteps, "num-steps", 1000, "Number of simulation time steps")
	flag.IntVar(&cfg.IngestionRebalanceInterval, "ingestion-rebalance-interval", 100, "Steps between ingestion rebalancing rounds")
	flag.IntVar(&cfg.QueryRebalanceInterval, "query-rebalance-interval", 10, "Steps between query rebalancing rounds")
	flag.Float64Var(&cfg.MergeChurnFraction, "merge-churn-fraction", 0.01, "Max fraction of hash space moved in merge phase")
	flag.Float64Var(&cfg.MoveChurnFraction, "move-churn-fraction", 0.09, "Max fraction of hash space moved in move phase")
	flag.IntVar(&cfg.MinRangesPerPartition, "min-ranges-per-partition", 50, "Min ranges per partition (merge threshold)")
	flag.IntVar(&cfg.MaxRangesPerPartition, "max-ranges-per-partition", 150, "Max ranges per partition (split threshold)")
	flag.IntVar(&cfg.IngestionMoveBudget, "ingestion-move-budget", 10, "Max hash-range moves per ingestion rebalance round")
	flag.IntVar(&cfg.QueryMoveBudget, "query-move-budget", 3, "Max partition moves per query rebalance round")
	flag.IntVar(&cfg.PartitionCatchupSteps, "partition-catchup-steps", 5, "Catch-up duration after partition move")
	flag.Float64Var(&cfg.CatchupCostFactor, "catchup-cost-factor", 1.5, "Replay overhead multiplier")
	flag.Float64Var(&cfg.BalanceTarget, "balance-target", 1.10, "Target max/mean ratio")
	flag.IntVar(&cfg.NumSyntheticSeries, "num-synthetic-series", 1000000, "Number of synthetic series")
	flag.IntVar(&cfg.NumSyntheticMetrics, "num-synthetic-metrics", 5000, "Number of synthetic metrics")
	flag.Float64Var(&cfg.SkewTopMetricFraction, "skew-top-metric-fraction", 0.80, "Fraction of series in top metric")
	flag.IntVar(&cfg.MetricHashBits, "metric-hash-bits", 14, "Number of high bits in the Nautilus hash reserved for metric name (label bits = 32 - M)")
	flag.Int64Var(&cfg.Seed, "seed", 42, "Random seed for reproducibility")
	flag.Parse()
	return cfg
}
