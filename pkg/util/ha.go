package util

import (
	"context"
	"time"
)

var (
	NoOpReplicaChecker = func(ctx context.Context, userID, cluster, replica string, now time.Time) error { return nil }
)
