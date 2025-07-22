package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus-community/parquet-common/schema"

	"github.com/grafana/mimir/pkg/storage/tsdb"
)

type ConvertConfig struct {
	Storage           tsdb.BlocksStorageConfig
	UsersRaw          string
	Verbose           bool
	LabelsCompression bool
	ChunksCompression bool
	LabelsCodec       string
	ChunksCodec       string

	// Parsed fields
	Users []string
}

func (c *ConvertConfig) RegisterFlags(fs *flag.FlagSet) {
	c.Storage.RegisterFlags(fs)
	fs.StringVar(&c.UsersRaw, "users", "", "Comma-separated list of users to convert (empty = all users)")
	fs.BoolVar(&c.Verbose, "verbose", false, "Enable verbose logging")
	fs.BoolVar(&c.LabelsCompression, "labels-compression", true, "Enable parquet compression for labels")
	fs.BoolVar(&c.ChunksCompression, "chunks-compression", true, "Enable parquet compression for chunks")
	fs.StringVar(&c.LabelsCodec, "labels-codec", "zstd", "Compression codec for labels (zstd, snappy)")
	fs.StringVar(&c.ChunksCodec, "chunks-codec", "zstd", "Compression codec for chunks (zstd, snappy)")
}

func (c *ConvertConfig) Validate() error {
	if c.UsersRaw != "" {
		c.Users = strings.Split(c.UsersRaw, ",")
		for i, user := range c.Users {
			c.Users[i] = strings.TrimSpace(user)
		}
	}

	if err := validateCodec(c.LabelsCodec); err != nil {
		return fmt.Errorf("invalid labels codec: %w", err)
	}

	if err := validateCodec(c.ChunksCodec); err != nil {
		return fmt.Errorf("invalid chunks codec: %w", err)
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
	LabelsCompression   bool
	ChunksCompression   bool
	LabelsCodec         string
	ChunksCodec         string

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
	fs.BoolVar(&c.LabelsCompression, "labels-compression", true, "Enable parquet compression for labels")
	fs.BoolVar(&c.ChunksCompression, "chunks-compression", true, "Enable parquet compression for chunks")
	fs.StringVar(&c.LabelsCodec, "labels-codec", "zstd", "Compression codec for labels (zstd, snappy)")
	fs.StringVar(&c.ChunksCodec, "chunks-codec", "zstd", "Compression codec for chunks (zstd, snappy)")
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

	if err := validateCodec(c.LabelsCodec); err != nil {
		return fmt.Errorf("invalid labels codec: %w", err)
	}

	if err := validateCodec(c.ChunksCodec); err != nil {
		return fmt.Errorf("invalid chunks codec: %w", err)
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

type AttributesGeneratorConfig struct {
	Storage        tsdb.BlocksStorageConfig
	Promote        bool
	BlockDirectory string
	AttributesRaw  string
	CardinalityRaw string
	Verbose        bool
	Cardinalities  map[string]int
}

const defaultAttributes = "cluster,container,instance,job,namespace,pod,service_name,telemetry_sdk_language,telemetry_sdk_name,telemetry_sdk_version,service_instance_id,service_version,deployment_environment_name,internal_grafana_com_promote_scope_attrs,k8s_cluster_name,k8s_container_name,k8s_deployment_name,k8s_namespace_name,k8s_node_name,k8s_pod_ip,k8s_pod_name,service_namespace"
const defaultCardinalities = "5,20,500,25,15,200,50,4,3,10,100,20,4,1,5,20,30,15,50,200,200,15"

func (c *AttributesGeneratorConfig) RegisterFlags(fs *flag.FlagSet) {
	c.Storage.RegisterFlags(fs)
	fs.StringVar(&c.BlockDirectory, "block-dir", "", "Directory containing the TSDB block to process")
	fs.BoolVar(&c.Promote, "promote", true, "false means attributes are put in target_info, true means they are promoted to labels")
	fs.StringVar(&c.AttributesRaw, "attributes", defaultAttributes, "Comma-separated attributes to generate")
	fs.StringVar(&c.CardinalityRaw, "cardinalities", defaultCardinalities, "Comma-separated cardinalities for each attribute")
	fs.BoolVar(&c.Verbose, "verbose", false, "Enable verbose logging")
}

func (c *AttributesGeneratorConfig) Validate() error {
	if c.BlockDirectory == "" {
		return fmt.Errorf("blocks-dir is required")
	}

	attributes := strings.Split(c.AttributesRaw, ",")
	cardinalityStrs := strings.Split(c.CardinalityRaw, ",")
	if len(attributes) != len(cardinalityStrs) {
		return fmt.Errorf("attributes and cardinalities must have the same length")
	}
	c.Cardinalities = make(map[string]int, len(attributes))
	for i, cardStr := range cardinalityStrs {
		val, err := strconv.Atoi(strings.TrimSpace(cardStr))
		if err != nil {
			return fmt.Errorf("invalid cardinality value: %s", cardStr)
		}
		c.Cardinalities[attributes[i]] = val
	}
	return nil
}

func validateCodec(codec string) error {
	switch strings.ToLower(codec) {
	case "zstd", "snappy":
		return nil
	default:
		return fmt.Errorf("unsupported codec %q, must be one of: zstd, snappy", codec)
	}
}

func parseCodec(codec string) schema.CompressionCodec {
	switch strings.ToLower(codec) {
	case "snappy":
		return schema.CompressionSnappy
	case "zstd":
		return schema.CompressionZstd
	default:
		// Default to zstd if somehow invalid codec gets through
		return schema.CompressionZstd
	}
}
