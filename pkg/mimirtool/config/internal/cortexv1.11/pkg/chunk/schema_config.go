// SPDX-License-Identifier: AGPL-3.0-only

package chunk

import (
	"errors"
	"flag"
	"time"

	"github.com/prometheus/common/model"
)

type PeriodConfig struct {
	From        DayTime             `yaml:"from"`         // used when working with config
	IndexType   string              `yaml:"store"`        // type of index client to use.
	ObjectType  string              `yaml:"object_store"` // type of object client to use; if omitted, defaults to store.
	Schema      string              `yaml:"schema"`
	IndexTables PeriodicTableConfig `yaml:"index"`
	ChunkTables PeriodicTableConfig `yaml:"chunks"`
	RowShards   uint32              `yaml:"row_shards"`
}

type SchemaConfig struct {
	Configs []PeriodConfig `yaml:"configs"`

	fileName string
}

type AutoScalingConfig struct {
	Enabled     bool    `yaml:"enabled"`
	RoleARN     string  `yaml:"role_arn"`
	MinCapacity int64   `yaml:"min_capacity"`
	MaxCapacity int64   `yaml:"max_capacity"`
	OutCooldown int64   `yaml:"out_cooldown"`
	InCooldown  int64   `yaml:"in_cooldown"`
	TargetValue float64 `yaml:"target"`
}

const (
	secondsInHour      = int64(time.Hour / time.Second)
	secondsInDay       = int64(24 * time.Hour / time.Second)
	millisecondsInHour = int64(time.Hour / time.Millisecond)
	millisecondsInDay  = int64(24 * time.Hour / time.Millisecond)
)

var (
	errInvalidSchemaVersion     = errors.New("invalid schema version")
	errInvalidTablePeriod       = errors.New("the table period must be a multiple of 24h (1h for schema v1)")
	errConfigFileNotSet         = errors.New("schema config file needs to be set")
	errConfigChunkPrefixNotSet  = errors.New("schema config for chunks is missing the 'prefix' setting")
	errSchemaIncreasingFromTime = errors.New("from time in schemas must be distinct and in increasing order")
)

type DayTime struct {
	model.Time
}
type PeriodicTableConfig struct {
	Prefix string
	Period time.Duration
	Tags   Tags
}

func (d *DayTime) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var from string
	if err := unmarshal(&from); err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02", from)
	if err != nil {
		return err
	}
	d.Time = model.TimeFromUnix(t.Unix())
	return nil
}
func (d *DayTime) String() string {
	return d.Time.Time().Format("2006-01-02")
}
func (cfg *SchemaConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.fileName, "schema-config-file", "", "The path to the schema config file. The schema config is used only when running Cortex with the chunks storage.")
}
func (cfg *PeriodicTableConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	g := struct {
		Prefix string         `yaml:"prefix"`
		Period model.Duration `yaml:"period"`
		Tags   Tags           `yaml:"tags"`
	}{}
	if err := unmarshal(&g); err != nil {
		return err
	}

	cfg.Prefix = g.Prefix
	cfg.Period = time.Duration(g.Period)
	cfg.Tags = g.Tags

	return nil
}
func (cfg *AutoScalingConfig) RegisterFlags(argPrefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, argPrefix+".enabled", false, "Should we enable autoscale for the table.")
	f.StringVar(&cfg.RoleARN, argPrefix+".role-arn", "", "AWS AutoScaling role ARN")
	f.Int64Var(&cfg.MinCapacity, argPrefix+".min-capacity", 3000, "DynamoDB minimum provision capacity.")
	f.Int64Var(&cfg.MaxCapacity, argPrefix+".max-capacity", 6000, "DynamoDB maximum provision capacity.")
	f.Int64Var(&cfg.OutCooldown, argPrefix+".out-cooldown", 1800, "DynamoDB minimum seconds between each autoscale up.")
	f.Int64Var(&cfg.InCooldown, argPrefix+".in-cooldown", 1800, "DynamoDB minimum seconds between each autoscale down.")
	f.Float64Var(&cfg.TargetValue, argPrefix+".target-value", 80, "DynamoDB target ratio of consumed capacity to provisioned capacity.")
}
