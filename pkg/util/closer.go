package util

// CloserFunc is like http.HandlerFunc but for io.Closer.
type CloserFunc func() error

// Close implements io.Closer.
func (f CloserFunc) Close() error {
	return f()
}
