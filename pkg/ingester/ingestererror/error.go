package ingestererror

import (
	"errors"
	"fmt"
)

const (
	integerUnavailableMsgFormat = "ingester is unavailable (current state: %s)"
)

// safeToWrap is an interface annotating errors that are safe to wrap.
type safeToWrap interface {
	safeToWrap()
}

// safeToWrapError is an error implementing the safeToWrap interface.
type safeToWrapError string

func (s safeToWrapError) safeToWrap() {}

func (s safeToWrapError) Error() string {
	return string(s)
}

func NewSafeToWrapError(format string, args ...any) error {
	return safeToWrapError(
		fmt.Sprintf(format, args...),
	)
}

// WrapOrAnnotateWithUser prepends the given userID to the given error.
// If the error is safe, the returned error retains a reference to the former.
func WrapOrAnnotateWithUser(err error, userID string) error {
	// If this is a safe error, we wrap it with userID and return it, because
	// it might contain extra information for gRPC and our logging middleware.
	var safe safeToWrap
	if errors.As(err, &safe) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}
	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// Unavailable is a safeToWrap error indicating that the ingester is unavailable.
type Unavailable struct {
	safeToWrapError
}

func NewUnavailableError(state string) Unavailable {
	return Unavailable{
		safeToWrapError(
			fmt.Sprintf(integerUnavailableMsgFormat, state),
		),
	}
}

// InstanceLimitReached is a safeToWrap error indicating that an instance limit has been reached.
type InstanceLimitReached struct {
	safeToWrapError
}

func NewInstanceLimitReachedError(message string) InstanceLimitReached {
	return InstanceLimitReached{
		safeToWrapError(
			message,
		),
	}
}

// TSDBUnavailable is a safeToWrap error indicating that the TSDB is unavailable.
type TSDBUnavailable struct {
	safeToWrapError
}

func NewTSDBUnavailableError(err error) TSDBUnavailable {
	return TSDBUnavailable{
		safeToWrapError(
			err.Error(),
		),
	}
}
