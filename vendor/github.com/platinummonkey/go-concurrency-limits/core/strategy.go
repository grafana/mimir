package core

import (
	"context"
)

// StrategyToken represents a token from a limiter algorithm
type StrategyToken interface {
	// IsAcquired returns true if acquired or false if limit has been reached.
	IsAcquired() bool
	// InFlightCount will return the number of pending requests.
	InFlightCount() int
	// Release the acquired token and decrement the current in-flight count.
	Release()
}

// StaticStrategyToken represents a static strategy token, simple but flexible.
type StaticStrategyToken struct {
	acquired      bool
	inFlightCount int
	releaseFunc   func()
}

// IsAcquired will return true if the token is acquired
func (t *StaticStrategyToken) IsAcquired() bool {
	return t.acquired
}

// InFlightCount represents the instantaneous snapshot on token creation in-flight count
func (t *StaticStrategyToken) InFlightCount() int {
	return t.inFlightCount
}

// Release will release the current token, it's very important to release all tokens!
func (t *StaticStrategyToken) Release() {
	if t.releaseFunc != nil {
		t.releaseFunc()
	}
}

// NewNotAcquiredStrategyToken will create a new un-acquired strategy token.
func NewNotAcquiredStrategyToken(inFlightCount int) StrategyToken {
	return &StaticStrategyToken{
		acquired:      false,
		inFlightCount: inFlightCount,
		releaseFunc:   func() {},
	}
}

// NewAcquiredStrategyToken will create a new acquired strategy token.
func NewAcquiredStrategyToken(inFlightCount int, releaseFunc func()) StrategyToken {
	return &StaticStrategyToken{
		acquired:      true,
		inFlightCount: inFlightCount,
		releaseFunc:   releaseFunc,
	}
}

// Strategy defines how the limiter logic should acquire or not acquire tokens.
type Strategy interface {
	// TryAcquire will try to acquire a token from the limiter.
	// context Context of the request for partitioned limits.
	// returns not ok if limit is exceeded, or a StrategyToken that must be released when the operation completes.
	TryAcquire(ctx context.Context) (token StrategyToken, ok bool)

	// SetLimit will update the strategy with a new limit.
	SetLimit(limit int)
}
