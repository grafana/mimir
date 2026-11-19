// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"golang.org/x/crypto/blake2b"
)

const (
	metadataCacheIDPrefix = "blocks-bucket-"

	// maxBucketNameLengthInMetadataCacheID keeps the metadata cache keys short. A longer bucket name is hashed.
	maxBucketNameLengthInMetadataCacheID = 63
)

// BlocksBucket is a bucket that holds blocks.
type BlocksBucket struct {
	// Name identifies the bucket in logs, metrics, cache keys and debug pages.
	Name string

	// MetadataCacheID prefixes the cache keys of the bucket metadata, such as the bucket index, listings,
	// meta.json files and block marks. It is empty when the set has only one bucket, so that the cache
	// keys of a cluster with one bucket stay the same.
	MetadataCacheID string

	Client objstore.Bucket
}

// BlocksBucketSet holds the write bucket and the additional read buckets of the blocks storage.
// Mimir writes new blocks to the write bucket and reads blocks from all buckets.
type BlocksBucketSet struct {
	writeBucket BlocksBucket

	// additionalReadBuckets are in read order. Mimir reads them after the write bucket.
	additionalReadBuckets []BlocksBucket
}

// NewBlocksBucketSet returns a set that holds the write bucket and the additional read buckets.
// NewBlocksBucketSet sets the MetadataCacheID of each bucket.
func NewBlocksBucketSet(writeBucket BlocksBucket, additionalReadBuckets []BlocksBucket) (*BlocksBucketSet, error) {
	names := make([]string, 0, 1+len(additionalReadBuckets))
	names = append(names, writeBucket.Name)
	for _, b := range additionalReadBuckets {
		names = append(names, b.Name)
	}
	if err := validateBlocksBucketNames(names); err != nil {
		return nil, err
	}

	hasMoreThanOneBucket := len(additionalReadBuckets) > 0
	withMetadataCacheID := func(b BlocksBucket) BlocksBucket {
		b.MetadataCacheID = ""
		if hasMoreThanOneBucket {
			b.MetadataCacheID = metadataCacheID(b.Name)
		}
		return b
	}

	set := &BlocksBucketSet{
		writeBucket:           withMetadataCacheID(writeBucket),
		additionalReadBuckets: make([]BlocksBucket, 0, len(additionalReadBuckets)),
	}
	for _, b := range additionalReadBuckets {
		set.additionalReadBuckets = append(set.additionalReadBuckets, withMetadataCacheID(b))
	}
	return set, nil
}

// NewBlocksBucketSetFromConfigs creates one client for the write bucket config and one client for each
// additional read bucket config. It returns a set that holds these clients.
func NewBlocksBucketSetFromConfigs(ctx context.Context, writeCfg Config, additionalReadCfgs []Config, component string, logger log.Logger, reg prometheus.Registerer) (*BlocksBucketSet, error) {
	names := make([]string, 0, 1+len(additionalReadCfgs))
	names = append(names, writeCfg.BucketName())
	for _, cfg := range additionalReadCfgs {
		names = append(names, cfg.BucketName())
	}
	// Validate the names before the clients are created, because each client registers metrics with its bucket name.
	if err := validateBlocksBucketNames(names); err != nil {
		return nil, err
	}

	newBlocksBucket := func(cfg Config) (BlocksBucket, error) {
		client, err := NewClient(ctx, cfg, component, logger, reg)
		if err != nil {
			return BlocksBucket{}, fmt.Errorf("create client for blocks bucket %q: %w", cfg.BucketName(), err)
		}
		return BlocksBucket{Name: cfg.BucketName(), Client: client}, nil
	}

	writeBucket, err := newBlocksBucket(writeCfg)
	if err != nil {
		return nil, err
	}
	additionalReadBuckets := make([]BlocksBucket, 0, len(additionalReadCfgs))
	for _, cfg := range additionalReadCfgs {
		b, err := newBlocksBucket(cfg)
		if err != nil {
			return nil, err
		}
		additionalReadBuckets = append(additionalReadBuckets, b)
	}

	return NewBlocksBucketSet(writeBucket, additionalReadBuckets)
}

// WriteBucket returns the bucket that Mimir writes new blocks to.
func (s *BlocksBucketSet) WriteBucket() BlocksBucket {
	return s.writeBucket
}

// ReadBuckets returns all buckets in read order: first the write bucket, then the additional read buckets.
func (s *BlocksBucketSet) ReadBuckets() []BlocksBucket {
	return append([]BlocksBucket{s.writeBucket}, s.additionalReadBuckets...)
}

// validateBlocksBucketNames checks the names of the buckets of a set. A set with only one bucket accepts
// any name, as Mimir did before it supported more than one bucket.
func validateBlocksBucketNames(names []string) error {
	if len(names) == 1 {
		return nil
	}

	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return errors.New("the name of a blocks bucket must not be empty when there is more than one blocks bucket")
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("the blocks bucket name %q is used more than once", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// metadataCacheID returns the metadata cache key prefix of the bucket. It hashes a bucket name that is long
// or that contains characters that are not valid in a Memcached key.
func metadataCacheID(bucketName string) string {
	validInKey := strings.IndexFunc(bucketName, func(r rune) bool {
		return r <= ' ' || r == unicode.MaxASCII || r > unicode.MaxASCII
	}) < 0
	if validInKey && len(bucketName) <= maxBucketNameLengthInMetadataCacheID {
		return metadataCacheIDPrefix + bucketName
	}

	sum := blake2b.Sum256([]byte(bucketName))
	return metadataCacheIDPrefix + base64.RawURLEncoding.EncodeToString(sum[:])
}
