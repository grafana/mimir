package util

import (
	"flag"
	"time"
)

// FlagSet is an interface for *flag.FlagSet.
// This allows building adapters for other flag parsers to be compatible with our configurations.
type FlagSet interface {
	BoolVar(p *bool, name string, value bool, usage string)
	IntVar(p *int, name string, value int, usage string)
	Int64Var(p *int64, name string, value int64, usage string)
	UintVar(p *uint, name string, value uint, usage string)
	Uint64Var(p *uint64, name string, value uint64, usage string)
	StringVar(p *string, name string, value string, usage string)
	Float64Var(p *float64, name string, value float64, usage string)
	DurationVar(p *time.Duration, name string, value time.Duration, usage string)
	Var(value flag.Value, name string, usage string)
}
