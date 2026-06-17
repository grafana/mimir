// SPDX-License-Identifier: AGPL-3.0-only

//go:build !linux && !darwin

package mimirpb

const refLeaksInstrumentationSupported = false

func mmapAnon(len int) ([]byte, error) {
	panic(errRefLeaksNotAvailable)
}

func mprotectNone(b []byte) error {
	panic(errRefLeaksNotAvailable)
}

func munmap(b []byte) error {
	panic(errRefLeaksNotAvailable)
}
