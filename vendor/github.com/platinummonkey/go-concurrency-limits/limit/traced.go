package limit

import (
	"fmt"
	"log"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// Logger implements a basic dependency to log. Feel free to report stats as well.
type Logger interface {
	// Log a Debug statement already formatted.
	Debugf(msg string, params ...interface{})
	// Check if debug is enabled
	IsDebugEnabled() bool
}

// NoopLimitLogger implements a NO-OP logger, it does nothing.
type NoopLimitLogger struct{}

// Debugf debug formatted log
func (l NoopLimitLogger) Debugf(msg string, params ...interface{}) {}

// IsDebugEnabled will return true if debug is enabled. NoopLimitLogger is always `false`
func (l NoopLimitLogger) IsDebugEnabled() bool {
	return false
}

func (l NoopLimitLogger) String() string {
	return "NoopLimitLogger{}"
}

// BuiltinLimitLogger implements a STDOUT limit logger.
type BuiltinLimitLogger struct{}

// Debugf debug formatted log
func (l BuiltinLimitLogger) Debugf(msg string, params ...interface{}) {
	log.Println(fmt.Sprintf(msg, params...))
}

// IsDebugEnabled will return true if debug is enabled. BuiltinLimitLogger is always `true`
func (l BuiltinLimitLogger) IsDebugEnabled() bool {
	return true
}

func (l BuiltinLimitLogger) String() string {
	return "BuiltinLimitLogger{}"
}

// TracedLimit implements core.Limit but adds some additional logging
type TracedLimit struct {
	limit  core.Limit
	logger Logger
}

// NewTracedLimit returns a new wrapped Limit with TracedLimit.
func NewTracedLimit(limit core.Limit, logger Logger) *TracedLimit {
	return &TracedLimit{
		limit:  limit,
		logger: logger,
	}
}

// EstimatedLimit returns the estimated limit.
func (l *TracedLimit) EstimatedLimit() int {
	estimatedLimit := l.limit.EstimatedLimit()
	l.logger.Debugf("estimatedLimit=%d\n", estimatedLimit)
	return estimatedLimit
}

// NotifyOnChange will register a callback to receive notification whenever the limit is updated to a new value.
func (l *TracedLimit) NotifyOnChange(consumer core.LimitChangeListener) {
	l.limit.NotifyOnChange(consumer)
}

// OnSample will log and deleate the update of the sample.
func (l *TracedLimit) OnSample(startTime int64, rtt int64, inFlight int, didDrop bool) {
	l.logger.Debugf("startTime=%d, rtt=%d ms, inFlight=%d, didDrop=%t", startTime, rtt/1e6, inFlight, didDrop)
	l.limit.OnSample(startTime, rtt, inFlight, didDrop)
}

func (l TracedLimit) String() string {
	return fmt.Sprintf("TracedLimit{limit=%v, logger=%v}", l.limit, l.logger)
}
