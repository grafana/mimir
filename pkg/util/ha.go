package util

import (
	"context"
	"fmt"
	"time"
)

var (
	NoOpReplicaChecker = func(ctx context.Context, userID, cluster, replica string, now time.Time) error { return nil }
)

type ReplicasNotMatchError struct {
	replica, elected string
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatchError {
	return ReplicasNotMatchError{
		replica: replica,
		elected: elected,
	}
}

func (e ReplicasNotMatchError) Error() string {
	return fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// Needed for errors.Is to work properly.
func (e ReplicasNotMatchError) Is(err error) bool {
	_, ok1 := err.(ReplicasNotMatchError)
	_, ok2 := err.(*ReplicasNotMatchError)
	return ok1 || ok2
}

// IsOperationAborted returns whether the error has been caused by an operation intentionally aborted.
func (e ReplicasNotMatchError) IsOperationAborted() bool {
	return true
}
