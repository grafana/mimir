package clusterutil

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

const (
	// ClusterVerificationLabelHeader is the name of the cluster verification label HTTP header.
	ClusterVerificationLabelHeader = "X-Cluster"

	// MetadataClusterVerificationLabelKey is the key of the cluster verification label gRPC metadata.
	MetadataClusterVerificationLabelKey = "x-cluster"
)

var (
	ErrNoClusterVerificationLabel               = errors.New("no cluster verification label in context")
	ErrDifferentClusterVerificationLabelPresent = errors.New("different cluster verification label already present in header")
)

func NewIncomingContext(containsRequestCluster bool, requestCluster string) context.Context {
	ctx := context.Background()
	if !containsRequestCluster {
		return ctx
	}
	md := map[string][]string{
		MetadataClusterVerificationLabelKey: {requestCluster},
	}
	return metadata.NewIncomingContext(ctx, md)
}

// PutClusterIntoOutgoingContext returns a new context with the provided value
// for MetadataClusterVerificationLabelKey merged with any existing metadata in the context.
// Empty values are ignored.
func PutClusterIntoOutgoingContext(ctx context.Context, cluster string) context.Context {
	if cluster == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterVerificationLabelKey, cluster)
}

// GetClusterFromIncomingContext returns a single metadata value corresponding to the
// key MetadataClusterVerificationLabelKey from the incoming context if it exists. In all other cases
// an error is returned.
func GetClusterFromIncomingContext(ctx context.Context, logger log.Logger) (string, error) {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterVerificationLabelKey)
	if len(clusterIDs) == 0 {
		return "", ErrNoClusterVerificationLabel
	}
	if len(clusterIDs) > 1 {
		if logger != nil {
			msg := fmt.Sprintf("gRPC metadata should contain exactly 1 value for key %q, but the current set of values is %v.", MetadataClusterVerificationLabelKey, clusterIDs)
			level.Warn(logger).Log("msg", msg)
		}
		return "", ErrDifferentClusterVerificationLabelPresent
	}
	return clusterIDs[0], nil
}
