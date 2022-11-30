// SPDX-License-Identifier: AGPL-3.0-only

package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// ParseSeekerBodyForm will ensure that request body is a Seeker, parse the form and seek the body back to the beginning.
func ParseSeekerBodyForm(r *http.Request) error {
	return withSeekerBody(r, func(r *http.Request) error {
		return r.ParseForm()
	})
}

// withSeekerBody will ensure that request body is a Seeker, call the provided function and seek the body back to the beginning.
func withSeekerBody(r *http.Request, f func(r *http.Request) error) (err error) {
	if r.Body == nil {
		return nil
	}
	seeker, ok := r.Body.(io.Seeker)
	if !ok {
		seeker, err = newReadCloseSeeker(r.Body)
		if err != nil {
			return fmt.Errorf("can't read body: %w", err)
		}
		if err := r.Body.Close(); err != nil {
			return fmt.Errorf("can't close original body: %w", err)
		}
	}
	defer func() {
		if err == nil {
			_, err = seeker.Seek(0, io.SeekStart)
		}
	}()

	return f(r)
}

// newReadCloseSeeker will create a new readCloseSeeker from the data provided by io.Reader.
func newReadCloseSeeker(r io.Reader) (readCloseSeeker, error) {
	buf, err := io.ReadAll(r)
	return readCloseSeeker{bytes.NewReader(buf)}, err
}

type readCloseSeeker struct{ *bytes.Reader }

func (readCloseSeeker) Close() error { return nil }
