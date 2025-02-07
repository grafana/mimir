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
	// ClusterHeader is the name of the cluster identifying HTTP header.
	ClusterHeader = "X-Cluster"

	// MetadataClusterKey is the key of the cluster gRPC metadata.
	MetadataClusterKey = "x-cluster"
)

var (
	ErrNoCluster               = errors.New("no cluster in context")
	ErrDifferentClusterPresent = errors.New("different cluster already present in header")
)

// PutClusterIntoOutgoingContext returns a new context with the provided value
// for MetadataClusterKey merged with any existing metadata in the context.
func PutClusterIntoOutgoingContext(ctx context.Context, cluster string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterKey, cluster)
}

// GetClusterFromIncomingContext returns the metadata value corresponding to the metadata
// key MetadataClusterKey from the incoming metadata if it exists.
func GetClusterFromIncomingContext(ctx context.Context, logger log.Logger) string {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterKey)
	if len(clusterIDs) != 1 {
		msg := fmt.Sprintf("gRPC metadata should contain exactly 1 value for key \"%s\", but the current set of values is %v. Returning an empty string.", MetadataClusterKey, clusterIDs)
		level.Warn(logger).Log("msg", msg)
		return ""
	}
	return clusterIDs[0]
}

// InjectCluster returns a derived context containing the cluster.
func InjectCluster(ctx context.Context, cluster string) context.Context {
	return context.WithValue(ctx, interface{}(MetadataClusterKey), cluster)
}

// ExtractCluster gets the cluster from the context.
func ExtractCluster(ctx context.Context) (string, error) {
	userID, ok := ctx.Value(MetadataClusterKey).(string)
	if !ok {
		return "", ErrNoCluster
	}
	return userID, nil
}

// InjectClusterIntoHTTPRequest injects the cluster from the context into the request headers.
func InjectClusterIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	userID, err := ExtractCluster(ctx)
	if err != nil {
		return err
	}
	existingID := r.Header.Get(ClusterHeader)
	if existingID != "" && existingID != userID {
		return ErrDifferentClusterPresent
	}
	r.Header.Set(ClusterHeader, userID)
	return nil
}
