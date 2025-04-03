package validation

import "time"

type LimitedQuery struct {
	Query        string        `yaml:"pattern"`
	MaxFrequency time.Duration `yaml:"frequency"` // query may only be run once per this duration
}
