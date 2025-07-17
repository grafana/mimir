package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/storage/tsdb"
)

type ConvertConfig struct {
	Storage  tsdb.BlocksStorageConfig
	UsersRaw string
	Verbose  bool

	// Parsed fields
	Users []string
}

func (c *ConvertConfig) RegisterFlags(fs *flag.FlagSet) {
	c.Storage.RegisterFlags(fs)
	fs.StringVar(&c.UsersRaw, "users", "", "Comma-separated list of users to convert (empty = all users)")
	fs.BoolVar(&c.Verbose, "verbose", false, "Enable verbose logging")
}

func (c *ConvertConfig) Validate() error {
	if c.UsersRaw != "" {
		c.Users = strings.Split(c.UsersRaw, ",")
		for i, user := range c.Users {
			c.Users[i] = strings.TrimSpace(user)
		}
	}
	return nil
}

type GenerateConfig struct {
	Storage             tsdb.BlocksStorageConfig
	UserID              string
	DPM                 int
	MetricNamesRaw      string
	LabelNamesRaw       string
	LabelCardinalityRaw string
	TimeRangeHours      int
	ScrapeInterval      time.Duration
	OutputPrefix        string
	Verbose             bool

	// Parsed fields
	MetricNames      []string
	LabelNames       []string
	LabelCardinality []int
}

func (c *GenerateConfig) RegisterFlags(fs *flag.FlagSet) {
	c.Storage.RegisterFlags(fs)
	fs.StringVar(&c.UserID, "user", "user-1", "User ID for dataset")
	fs.IntVar(&c.DPM, "dpm", 1, "Datapoints per minute")
	fs.StringVar(&c.MetricNamesRaw, "metric-names", "cpu_usage,memory_usage,disk_io", "Comma-separated metric names")
	fs.StringVar(&c.LabelNamesRaw, "label-names", "instance,job,region", "Comma-separated label names")
	fs.StringVar(&c.LabelCardinalityRaw, "label-cardinality", "10,5,3", "Comma-separated cardinality for each label")
	fs.IntVar(&c.TimeRangeHours, "time-range-hours", 24, "Time range in hours")
	fs.DurationVar(&c.ScrapeInterval, "scrape-interval", 15*time.Second, "Scrape interval")
	fs.StringVar(&c.OutputPrefix, "output-prefix", "benchmark-dataset", "Prefix for generated blocks")
	fs.BoolVar(&c.Verbose, "verbose", false, "Enable verbose logging")
}

func (c *GenerateConfig) Validate() error {
	c.MetricNames = strings.Split(c.MetricNamesRaw, ",")
	c.LabelNames = strings.Split(c.LabelNamesRaw, ",")
	cardinalityStr := strings.Split(c.LabelCardinalityRaw, ",")

	if len(c.LabelNames) != len(cardinalityStr) {
		return fmt.Errorf("label-names and label-cardinality must have the same length")
	}

	c.LabelCardinality = make([]int, len(cardinalityStr))
	for i, cardStr := range cardinalityStr {
		val, err := strconv.Atoi(strings.TrimSpace(cardStr))
		if err != nil {
			return fmt.Errorf("invalid cardinality value: %s", cardStr)
		}
		c.LabelCardinality[i] = val
	}

	return nil
}

type PromoterConfig struct {
	Storage         tsdb.BlocksStorageConfig
	BlocksDirectory string
	Verbose         bool
}

func (c *PromoterConfig) RegisterFlags(fs *flag.FlagSet) {
	c.Storage.RegisterFlags(fs)
	fs.StringVar(&c.BlocksDirectory, "blocks-dir", "", "Directory containing TSDB blocks to process")
	fs.BoolVar(&c.Verbose, "verbose", false, "Enable verbose logging")
}

func (c *PromoterConfig) Validate() error {
	if c.BlocksDirectory == "" {
		return fmt.Errorf("blocks-dir is required")
	}
	return nil
}
