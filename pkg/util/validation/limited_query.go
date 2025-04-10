package validation

import "time"

type LimitedQuery struct {
	Query            string        `yaml:"query"`
	AllowedFrequency time.Duration `yaml:"allowed_frequency"` // query may only be run once per this duration
}
