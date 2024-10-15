// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"encoding/json"
	"expvar"
	"fmt"
	"time"

	"go.uber.org/atomic"
)

const (
	statsPrefix = "github.com/grafana/mimir/"
	targetKey   = "target"
	editionKey  = "edition"

	EditionOSS        = "oss"
	EditionEnterprise = "enterprise"
)

func init() {
	SetEdition(EditionOSS)
}

// SetTarget sets the target name.
func SetTarget(target string) {
	GetString(targetKey).Set(target)
}

// SetEdition sets the edition name.
func SetEdition(edition string) {
	GetString(editionKey).Set(edition)
}

// GetString returns the String stats object for the given name.
// It creates the stats object if it doesn't exist yet.
func GetString(name string) *expvar.String {
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if s, ok := existing.(*expvar.String); ok {
			return s
		}
		panic(fmt.Sprintf("%v is set to a non-string value", name))
	}
	return expvar.NewString(statsPrefix + name)
}

// GetInt returns a new Int stats object for the given name.
// It creates the stats object if it doesn't exist yet.
func GetInt(name string) *expvar.Int {
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if i, ok := existing.(*expvar.Int); ok {
			return i
		}
		panic(fmt.Sprintf("%v is set to a non-int value", name))
	}
	return expvar.NewInt(statsPrefix + name)
}

// GetAndResetInt calls GetInt and then reset it to 0.
func GetAndResetInt(name string) *expvar.Int {
	stat := GetInt(name)
	stat.Set(0)
	return stat
}

// GetFloat returns a new Float stats object for the given name.
// It creates the stats object if it doesn't exist yet.
func GetFloat(name string) *expvar.Float {
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if i, ok := existing.(*expvar.Float); ok {
			return i
		}
		panic(fmt.Sprintf("%v is set to a non-float value", name))
	}
	return expvar.NewFloat(statsPrefix + name)
}

// GetAndResetFloat calls GetFloat and then reset it to 0.
func GetAndResetFloat(name string) *expvar.Float {
	stat := GetFloat(name)
	stat.Set(0)
	return stat
}

type Counter struct {
	total     *atomic.Int64
	rate      *atomic.Float64
	resetTime *atomic.Time
}

// GetCounter returns a new Counter stats object for the given name.
// It creates the stats object if it doesn't exist yet.
func GetCounter(name string) *Counter {
	c := &Counter{
		total:     atomic.NewInt64(0),
		rate:      atomic.NewFloat64(0),
		resetTime: atomic.NewTime(time.Now()),
	}
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if c, ok := existing.(*Counter); ok {
			return c
		}
		panic(fmt.Sprintf("%v is set to a non-Counter value", name))
	}
	expvar.Publish(statsPrefix+name, c)
	return c
}

// GetAndResetCounter calls GetCounter and then reset it to 0.
func GetAndResetCounter(name string) *Counter {
	stat := GetCounter(name)
	stat.reset()
	return stat
}

func (c *Counter) updateRate() {
	total := c.total.Load()
	c.rate.Store(float64(total) / time.Since(c.resetTime.Load()).Seconds())
}

func (c *Counter) reset() {
	c.total.Store(0)
	c.rate.Store(0)
	c.resetTime.Store(time.Now())
}

func (c *Counter) Inc(i int64) {
	c.total.Add(i)
}

func (c *Counter) String() string {
	b, _ := json.Marshal(c.Value())
	return string(b)
}

func (c *Counter) Total() int64 {
	return c.total.Load()
}

func (c *Counter) Value() map[string]interface{} {
	return map[string]interface{}{
		"total": c.total.Load(),
		"rate":  c.rate.Load(),
	}
}
