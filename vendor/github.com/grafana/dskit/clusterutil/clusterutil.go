package clusterutil

import (
	"context"
	"fmt"
	"net/http"

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

type clusterContextKey string

// PutClusterIntoOutgoingContext returns a new context with the provided value
// for MetadataClusterVerificationLabelKey merged with any existing metadata in the context.
func PutClusterIntoOutgoingContext(ctx context.Context, cluster string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterVerificationLabelKey, cluster)
}

// GetClusterFromIncomingContext returns the metadata value corresponding to the metadata
// key MetadataClusterVerificationLabelKey from the incoming metadata if it exists.
func GetClusterFromIncomingContext(ctx context.Context, logger log.Logger) string {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterVerificationLabelKey)
	if len(clusterIDs) != 1 {
		msg := fmt.Sprintf("gRPC metadata should contain exactly 1 value for key %q, but the current set of values is %v. Returning an empty string.", MetadataClusterVerificationLabelKey, clusterIDs)
		level.Warn(logger).Log("msg", msg)
		return ""
	}
	return clusterIDs[0]
}

// InjectCluster returns a derived context containing the cluster.
func InjectCluster(ctx context.Context, cluster string) context.Context {
	return context.WithValue(ctx, clusterContextKey(MetadataClusterVerificationLabelKey), cluster)
}

// ExtractCluster gets the cluster from the context.
func ExtractCluster(ctx context.Context) (string, error) {
	userID := ctx.Value(clusterContextKey(MetadataClusterVerificationLabelKey)).(string)
	if userID == "" {
		return "", ErrNoClusterVerificationLabel
	}
	return userID, nil
}

// InjectClusterIntoHTTPRequest injects the cluster from the context into the request headers.
func InjectClusterIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	userID, err := ExtractCluster(ctx)
	if err != nil {
		return err
	}
	existingID := r.Header.Get(ClusterVerificationLabelHeader)
	if existingID != "" && existingID != userID {
		return ErrDifferentClusterVerificationLabelPresent
	}
	r.Header.Set(ClusterVerificationLabelHeader, userID)
	return nil
}
