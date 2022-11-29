package ring

import (
    "context"
    "errors"
)

// ErrTransferDisabled is the error returned by TransferOut when the transfers are disabled.
var ErrTransferDisabled = errors.New("transfers disabled")

// FlushTransferer controls the shutdown of an instance in the ring.
// Methods on this interface are called when lifecycler is stopping.
// At that point, lifecycler no longer runs the "actor loop", but it keeps updating heartbeat in the ring.
// Ring entry is in LEAVING state.
//
// Lifecycler calls TransferOut first. If transfer succeeds, no more methods are called.
// If TransferOut returns error and lifecycler's FlushOnShutdown flag is set to true, lifecycler then calls Flush.
// If transferer implements FlushTransferer2, FlushWithError is used instead of Flush, and TransferAfterFlush is called if Flush succeeds.
/*
        ┌─────────────────┐
        │  TransferOut    │
        └─────────────────┘
                  │
                  │
              transfer
             failed and
             flushing is
               enabled
                  │
                  │
                  ▼
            ┌──────────┐
            │  Flush   │
            └──────────┘
*/
type FlushTransferer interface {
    // TransferOut is called before Flush.
    TransferOut(ctx context.Context) error

	// Flush is only called if TransferOut returned error AND lifecycler's
	// FlushOnShutdown is set to true.
	Flush()
}

// Extension of FlushTransferer interface. When transferer implements FlushTransferer2, following methods are called:
/*
           ┌─────────────────┐
           │   TransferOut   │
           └─────────────────┘
                    │
                    │
                transfer
               failed and
               flushing is
                 enabled
                    │
                    ▼
          ┌──────────────────┐
          │  FlushWithError  │
          └──────────────────┘
                    │
                 no error
                    │
                    ▼
        ┌──────────────────────┐
        │  TransferAfterFlush  │
        └──────────────────────┘
 */
type FlushTransferer2 interface {
    FlushTransferer

    // This method is called instead of Flush from FlushTransferer. Similar to Flush,
	// this is only called if TransferOut returned error AND lifecycler's
	// FlushOnShutdown is set to true.
    FlushWithError(ctx context.Context) error

    // TransferAfterFlush is called if FlushWithError returned without error.
    TransferAfterFlush(ctx context.Context) error
}

// NoopFlushTransferer is a FlushTransferer which does nothing and can
// be used in cases we don't need one
type NoopFlushTransferer struct{}

// NewNoopFlushTransferer makes a new NoopFlushTransferer
func NewNoopFlushTransferer() *NoopFlushTransferer {
	return &NoopFlushTransferer{}
}

// Flush is a noop
func (t *NoopFlushTransferer) Flush() {}

// TransferOut is a noop
func (t *NoopFlushTransferer) TransferOut(ctx context.Context) error {
	return nil
}
