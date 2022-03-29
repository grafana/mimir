package main

import (
	"flag"
	"fmt"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

type kingPinFlagSet struct {
	app *kingpin.Application
}

func (kfs kingPinFlagSet) BoolVar(p *bool, name string, value bool, usage string) {
	kfs.app.Flag(name, usage).
		Default(fmt.Sprintf("%t", value)).
		BoolVar(p)
}

func (kfs kingPinFlagSet) IntVar(p *int, name string, value int, usage string) {
	kfs.app.Flag(name, usage).
		Default(fmt.Sprintf("%d", value)).
		IntVar(p)
}

func (kfs kingPinFlagSet) Int64Var(p *int64, name string, value int64, usage string) {
	kfs.app.Flag(name, usage).
		Default(fmt.Sprintf("%d", value)).
		Int64Var(p)
}

func (kfs kingPinFlagSet) UintVar(p *uint, name string, value uint, usage string) {
	kfs.app.Flag(name, usage).
		Default(fmt.Sprintf("%d", value)).
		UintVar(p)
}

func (kfs kingPinFlagSet) Uint64Var(p *uint64, name string, value uint64, usage string) {
	kfs.app.Flag(name, usage).
		Default(fmt.Sprintf("%d", value)).
		Uint64Var(p)
}

func (kfs kingPinFlagSet) StringVar(p *string, name string, value string, usage string) {
	kfs.app.Flag(name, usage).
		Default(value).
		StringVar(p)
}

func (kfs kingPinFlagSet) Float64Var(p *float64, name string, value float64, usage string) {
	var def string
	if float64(int64(value)) == value {
		def = fmt.Sprintf("%f.0", value)
	} else {
		def = fmt.Sprintf("%f", value)
	}
	kfs.app.Flag(name, usage).
		Default(def).
		FloatVar(p)
}

func (kfs kingPinFlagSet) DurationVar(p *time.Duration, name string, value time.Duration, usage string) {
	kfs.app.Flag(name, usage).
		Default(value.String()).
		DurationVar(p)
}

func (kfs kingPinFlagSet) Var(value flag.Value, name string, usage string) {
	kfs.app.Flag(name, usage).
		Default(value.String()).
		SetValue(value)
}
