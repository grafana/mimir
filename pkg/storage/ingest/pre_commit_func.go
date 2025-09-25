package ingest

type PreCommitFunc func() error

var NoOpPreCommitFunc = func() error { return nil }
