package logging

type LoggerFactory func(loggerName string, ctx ...interface{}) Logger

type Logger interface {
	// New returns a new contextual Logger that has this logger's context plus the given context.
	New(ctx ...interface{}) Logger

	Log(keyvals ...interface{}) error

	// Debug logs a message with debug level and key/value pairs, if any.
	Debug(msg string, ctx ...interface{})

	// Info logs a message with info level and key/value pairs, if any.
	Info(msg string, ctx ...interface{})

	// Warn logs a message with warning level and key/value pairs, if any.
	Warn(msg string, ctx ...interface{})

	// Error logs a message with error level and key/value pairs, if any.
	Error(msg string, ctx ...interface{})
}

type FakeLogger struct {
}

//nolint:revive
func (f FakeLogger) New(ctx ...interface{}) Logger {
	return f
}

//nolint:revive
func (f FakeLogger) Log(keyvals ...interface{}) error {
	return nil
}

//nolint:revive
func (f FakeLogger) Debug(msg string, ctx ...interface{}) {
}

//nolint:revive
func (f FakeLogger) Info(msg string, ctx ...interface{}) {
}

//nolint:revive
func (f FakeLogger) Warn(msg string, ctx ...interface{}) {
}

//nolint:revive
func (f FakeLogger) Error(msg string, ctx ...interface{}) {
}
