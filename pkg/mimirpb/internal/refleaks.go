// SPDX-License-Identifier: AGPL-3.0-only

package internal

import (
	"context"
	"reflect"
	"testing"
)

type RefLeakCheck struct {
	Addr   uintptr
	Leaked bool
}

var RefLeakChecks chan RefLeakCheck

func init() {
	if testing.Testing() {
		RefLeakChecks = make(chan RefLeakCheck)
	}
}

// NextRefLeakCheck is documented in testutil.NextRefLeakCheck.
func NextRefLeakCheck(ctx context.Context, object any) <-chan bool {
	if !testing.Testing() {
		panic("NextRefLeakCheck can only be used in tests")
	}
	var addr uintptr
	switch o := reflect.ValueOf(object); o.Type().Kind() {
	case reflect.Pointer, reflect.Slice, reflect.String:
		addr = uintptr(o.UnsafePointer())
	}

	ch := make(chan bool, 1)
	go func() {
		defer close(ch)
		for {
			select {
			case leak := <-RefLeakChecks:
				if leak.Addr == addr {
					ch <- leak.Leaked
					return
				}
			case <-ctx.Done():
			}
		}
	}()
	return ch
}
