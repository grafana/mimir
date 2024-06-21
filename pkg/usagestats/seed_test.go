// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestReadSeedFile(t *testing.T) {
	tests := map[string]struct {
		setup        func(bucketClient *bucket.ClientMock)
		expectedSeed ClusterSeed
		expectedErr  error
	}{
		"the seed file does not exist": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockGet(ClusterSeedFileName, "", bucket.ErrObjectDoesNotExist)
			},
			expectedErr: bucket.ErrObjectDoesNotExist,
		},
		"an error occurred while reading the seed file": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockGet(ClusterSeedFileName, "{}", errors.New("read failure"))
			},
			expectedErr: errors.New("read failure"),
		},
		"the seed file is corrupted": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockGet(ClusterSeedFileName, "xxx", nil)
			},
			expectedErr: errClusterSeedFileCorrupted,
		},
		"the seed file is read successfully": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockGet(ClusterSeedFileName, `{"UID":"xxx","created_at":"2006-01-02T15:04:05.999999999Z"}`, nil)
			},
			expectedSeed: ClusterSeed{
				UID: "xxx",
				CreatedAt: func() time.Time {
					ts, err := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z")
					if err != nil {
						panic(err)
					}
					return ts
				}(),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			testData.setup(bucketClient)

			seed, err := readSeedFile(context.Background(), objstore.WrapWithMetrics(bucketClient, nil, ""), log.NewNopLogger())
			if testData.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, testData.expectedSeed, seed)
			}
		})
	}
}

func TestWriteSeedFile(t *testing.T) {
	seed := ClusterSeed{
		UID:       "xxx",
		CreatedAt: time.Now(),
	}

	tests := map[string]struct {
		setup       func(bucketClient *bucket.ClientMock)
		expectedErr error
	}{
		"an error occurred while writing the seed file": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockUpload(ClusterSeedFileName, errors.New("write failure"))
			},
			expectedErr: errors.New("write failure"),
		},
		"the seed file is written successfully": {
			setup: func(bucketClient *bucket.ClientMock) {
				bucketClient.MockUpload(ClusterSeedFileName, nil)
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			testData.setup(bucketClient)

			err := writeSeedFile(context.Background(), objstore.WrapWithMetrics(bucketClient, nil, ""), seed)
			if testData.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWaitSeedFileStability(t *testing.T) {
	const minStability = 3 * time.Second

	type testExpectations struct {
		expectedSeed        ClusterSeed
		expectedErr         error
		expectedMinDuration time.Duration
	}

	tests := map[string]func(t *testing.T, bucketClient *bucket.ClientMock) testExpectations{
		"should immediately return if seed file does not exist": func(_ *testing.T, bucketClient *bucket.ClientMock) testExpectations {
			bucketClient.MockGet(ClusterSeedFileName, "", bucket.ErrObjectDoesNotExist)

			return testExpectations{
				expectedErr: bucket.ErrObjectDoesNotExist,
			}
		},
		"should immediately return if seed file is corrupted": func(_ *testing.T, bucketClient *bucket.ClientMock) testExpectations {
			bucketClient.MockGet(ClusterSeedFileName, "xxx", nil)

			return testExpectations{
				expectedErr: errClusterSeedFileCorrupted,
			}
		},
		"should immediately return if seed file was created more than 'min stability' time ago": func(t *testing.T, bucketClient *bucket.ClientMock) testExpectations {
			oldSeed := ClusterSeed{UID: "old", CreatedAt: time.Now().Add(-2 * minStability)}

			data, err := json.Marshal(oldSeed)
			require.NoError(t, err)
			bucketClient.MockGet(ClusterSeedFileName, string(data), nil)

			return testExpectations{
				expectedSeed:        oldSeed,
				expectedMinDuration: 0,
			}
		},
		"should wait for 'min stability' and return the seed file if was created less than 'min stability' time ago": func(t *testing.T, bucketClient *bucket.ClientMock) testExpectations {
			newSeed := ClusterSeed{UID: "new", CreatedAt: time.Now()}

			data, err := json.Marshal(newSeed)
			require.NoError(t, err)
			bucketClient.MockGet(ClusterSeedFileName, string(data), nil)

			return testExpectations{
				expectedSeed:        newSeed,
				expectedMinDuration: minStability,
			}
		},
	}

	for testName, testSetup := range tests {
		testSetup := testSetup

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			startTime := time.Now()

			bucketClient := &bucket.ClientMock{}
			testData := testSetup(t, bucketClient)

			actualSeed, err := waitSeedFileStability(context.Background(), objstore.WrapWithMetrics(bucketClient, nil, ""), minStability, log.NewNopLogger())
			if testData.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, testData.expectedSeed.UID, actualSeed.UID)
				require.Equal(t, testData.expectedSeed.CreatedAt.Unix(), actualSeed.CreatedAt.Unix())
			}

			require.GreaterOrEqual(t, time.Since(startTime), testData.expectedMinDuration)
		})
	}
}

func TestInitSeedFile(t *testing.T) {
	const minStability = 3 * time.Second

	type testExpectations struct {
		expectedErr         error
		expectedMinDuration time.Duration
	}

	tests := map[string]func(t *testing.T, bucketClient objstore.Bucket) testExpectations{
		"should immediately return if seed file exists and it was created more than 'min stability' time ago": func(t *testing.T, bucketClient objstore.Bucket) testExpectations {
			oldSeed := ClusterSeed{UID: "old", CreatedAt: time.Now().Add(-2 * minStability)}

			data, err := json.Marshal(oldSeed)
			require.NoError(t, err)
			require.NoError(t, bucketClient.Upload(context.Background(), ClusterSeedFileName, bytes.NewReader(data)))

			return testExpectations{
				expectedMinDuration: 0,
			}
		},
		"should wait for 'min stability' and return the seed file if it exists and was created less than 'min stability' time ago": func(t *testing.T, bucketClient objstore.Bucket) testExpectations {
			newSeed := ClusterSeed{UID: "new", CreatedAt: time.Now()}

			data, err := json.Marshal(newSeed)
			require.NoError(t, err)
			require.NoError(t, bucketClient.Upload(context.Background(), ClusterSeedFileName, bytes.NewReader(data)))

			return testExpectations{
				expectedMinDuration: minStability,
			}
		},
		"should create the seed file if doesn't exist and then wait for 'min stability'": func(*testing.T, objstore.Bucket) testExpectations {
			return testExpectations{
				expectedMinDuration: minStability,
			}
		},
		"should re-create the seed file if exist but is corrupted, and then wait for 'min stability'": func(t *testing.T, bucketClient objstore.Bucket) testExpectations {
			require.NoError(t, bucketClient.Upload(context.Background(), ClusterSeedFileName, bytes.NewReader([]byte("xxx"))))

			return testExpectations{
				expectedMinDuration: minStability,
			}
		},
	}

	for testName, testSetup := range tests {
		testSetup := testSetup

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			bucketClient := prepareLocalBucketClient(t)
			testData := testSetup(t, bucketClient)

			startTime := time.Now()
			actualSeed, err := initSeedFile(context.Background(), bucketClient, minStability, log.NewNopLogger())
			if testData.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), testData.expectedErr.Error())
			} else {
				require.NoError(t, err)

				// We expect the seed stored in the bucket.
				expectedSeed, err := readSeedFile(context.Background(), bucketClient, log.NewNopLogger())
				require.NoError(t, err)
				require.Equal(t, expectedSeed.UID, actualSeed.UID)
				require.Equal(t, expectedSeed.CreatedAt.Unix(), actualSeed.CreatedAt.Unix())
			}

			require.GreaterOrEqual(t, time.Since(startTime), testData.expectedMinDuration)
		})
	}
}

func TestInitSeedFile_CreatingConcurrency(t *testing.T) {
	t.Parallel()

	const (
		numReplicas  = 100
		minStability = 5 * time.Second
	)

	var (
		start   = make(chan struct{})
		seedsMx sync.Mutex
		seeds   = make([]ClusterSeed, 0, numReplicas)
	)

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	// Add a random delay to each API call to increase the likelihood of having multiple replicas creating the seed file.
	bucketClient = bucket.NewDelayedBucketClient(bucketClient, 5*time.Millisecond, 10*time.Millisecond)

	// Run replicas.
	group := errgroup.Group{}

	for i := 0; i < numReplicas; i++ {
		group.Go(func() error {
			// Wait for the start.
			<-start

			seed, err := initSeedFile(context.Background(), objstore.WrapWithMetrics(bucketClient, nil, ""), minStability, log.NewNopLogger())
			if err != nil {
				return err
			}

			seedsMx.Lock()
			seeds = append(seeds, seed)
			seedsMx.Unlock()

			return nil
		})
	}

	// Notify replicas to call initSeedFile().
	close(start)

	// Wait until all replicas have done.
	require.NoError(t, group.Wait())

	// We expect all replicas got the same seed.
	require.Len(t, seeds, numReplicas)
	for i := 1; i < numReplicas; i++ {
		require.Equal(t, seeds[0], seeds[i])
	}
}
