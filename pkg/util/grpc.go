// SPDX-License-Identifier: AGPL-3.0-only

package util

type Stream[T any] interface {
	CloseSend() error
	Recv() (T, error)
}

func CloseAndExhaust[T any](stream Stream[T]) error {
	if err := stream.CloseSend(); err != nil {
		return err
	}

	// Exhaust the stream to ensure:
	// - the gRPC library can release any resources associated with the stream (see https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream)
	// - instrumentation middleware correctly observes the end of the stream, rather than reporting it as "context canceled"
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}

	return nil
}
