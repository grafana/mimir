package clusterutil

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const (
	// MetadataClusterVerificationLabelKey is the key of the cluster verification label gRPC metadata.
	MetadataClusterVerificationLabelKey = "x-cluster"
)

var (
	ErrNoClusterVerificationLabel         = fmt.Errorf("no cluster verification label in context")
	errDifferentClusterVerificationLabels = func(clusterIDs []string) error {
		return fmt.Errorf("gRPC metadata should contain exactly 1 value for key %q, but it contains %v", MetadataClusterVerificationLabelKey, clusterIDs)
	}
)

// PutClusterIntoOutgoingContext returns a new context with the provided value for
// MetadataClusterVerificationLabelKey, merged with any existing metadata in the context.
// Empty values are ignored.
func PutClusterIntoOutgoingContext(ctx context.Context, cluster string) context.Context {
	if cluster == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterVerificationLabelKey, cluster)
}

// GetClusterFromIncomingContext returns a single metadata value corresponding to the
// MetadataClusterVerificationLabelKey key from the incoming context, if it exists.
// In all other cases an error is returned.
func GetClusterFromIncomingContext(ctx context.Context) (string, error) {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterVerificationLabelKey)
	if len(clusterIDs) > 1 {
		return "", errDifferentClusterVerificationLabels(clusterIDs)
	}
	if len(clusterIDs) == 0 || clusterIDs[0] == "" {
		return "", ErrNoClusterVerificationLabel
	}
	return clusterIDs[0], nil
}
