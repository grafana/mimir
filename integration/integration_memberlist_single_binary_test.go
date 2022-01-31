// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/integration_memberlist_single_binary_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/integration/ca"
	"github.com/grafana/mimir/integration/e2emimir"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testSingleBinaryEnv(t, false, nil)
	})

	t.Run("tls", func(t *testing.T) {
		testSingleBinaryEnv(t, true, nil)
	})

	t.Run("compression-disabled", func(t *testing.T) {
		testSingleBinaryEnv(t, false, map[string]string{
			"-memberlist.compression-enabled": "false",
		})
	})
}

func testSingleBinaryEnv(t *testing.T, tlsEnabled bool, flags map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	var mimir1, mimir2, mimir3 *e2emimir.MimirService
	if tlsEnabled {
		var (
			memberlistDNS = "cortex-memberlist"
		)
		// set the ca
		cert := ca.New("single-binary-memberlist")

		// Ensure the entire path of directories exist.
		require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))
		require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))
		require.NoError(t, cert.WriteCertificate(
			&x509.Certificate{
				Subject: pkix.Name{CommonName: "memberlist"},
				DNSNames: []string{
					memberlistDNS,
				},
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			},
			filepath.Join(s.SharedDir(), clientCertFile),
			filepath.Join(s.SharedDir(), clientKeyFile),
		))

		mimir1 = newSingleBinary("mimir-1", memberlistDNS, "", flags)
		mimir2 = newSingleBinary("mimir-2", memberlistDNS, networkName+"-mimir-1:8000", flags)
		mimir3 = newSingleBinary("mimir-3", memberlistDNS, networkName+"-mimir-1:8000", flags)
	} else {
		mimir1 = newSingleBinary("mimir-1", "", "", flags)
		mimir2 = newSingleBinary("mimir-2", "", networkName+"-mimir-1:8000", flags)
		mimir3 = newSingleBinary("mimir-3", "", networkName+"-mimir-1:8000", flags)
	}

	// start mimir-1 first, as mimir-2 and mimir-3 both connect to mimir-1
	require.NoError(t, s.StartAndWaitReady(mimir1))
	require.NoError(t, s.StartAndWaitReady(mimir2, mimir3))

	// All three Mimir serves should see each other.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))

	// Each Mimir instance will use (512 ingester tokens + 1 distributor token + 512 compactor tokens).
	tokensPerInstance := float64(512 + 1 + 512)
	// 3 = number of instances.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(3*tokensPerInstance), "cortex_ring_tokens_total"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(3*tokensPerInstance), "cortex_ring_tokens_total"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(3*tokensPerInstance), "cortex_ring_tokens_total"))

	// All Mimir servers should initially have no tombstones; nobody has left yet.
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))

	// Tombstones should increase by three for each server that leaves (once for distributor, ingester and compactor ring)
	require.NoError(t, s.Stop(mimir1))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(2*tokensPerInstance), "cortex_ring_tokens_total"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(3), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(2*tokensPerInstance), "cortex_ring_tokens_total"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(3), "memberlist_client_kv_store_value_tombstones"))

	require.NoError(t, s.Stop(mimir2))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(1*tokensPerInstance), "cortex_ring_tokens_total"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(1), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir3.WaitSumMetrics(e2e.Equals(6), "memberlist_client_kv_store_value_tombstones"))

	require.NoError(t, s.Stop(mimir3))
}

func newSingleBinary(name string, servername string, join string, testFlags map[string]string) *e2emimir.MimirService {
	flags := map[string]string{
		"-ingester.final-sleep":              "0s",
		"-ingester.join-after":               "0s", // join quickly
		"-ingester.min-ready-duration":       "0s",
		"-ingester.num-tokens":               "512",
		"-ring.store":                        "memberlist",
		"-memberlist.bind-port":              "8000",
		"-memberlist.left-ingesters-timeout": "600s", // effectively disable
	}

	if join != "" {
		flags["-memberlist.join"] = join
		flags["-ingester.observe-period"] = "5s" // Observe ring tokens to avoid conflicts.
	} else {
		flags["-ingester.observe-period"] = "0s" // No need to observe tokens because we're going to be the first instance.
	}

	serv := e2emimir.NewSingleBinary(
		name,
		mergeFlags(
			BlocksStorageFlags(),
			flags,
			testFlags,
			getTLSFlagsWithPrefix("memberlist", servername, servername == ""),
		),
		"",
		8000,
	)

	backOff := backoff.Config{
		MinBackoff: 200 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond, // Bump max backoff... things take little longer with memberlist.
		MaxRetries: 100,
	}

	serv.SetBackoff(backOff)
	return serv
}

func TestSingleBinaryWithMemberlistScaling(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Scale up instances. These numbers seem enough to reliably reproduce some unwanted
	// consequences of slow propagation, such as missing tombstones.
	maxMimir := 20
	minMimir := 3
	instances := make([]*e2emimir.MimirService, 0)

	// Start the 1st instance. This will provide the initial state to other members.
	firstInstance := newSingleBinary("mimir-1", "", "", nil)
	require.NoError(t, s.StartAndWaitReady(firstInstance))
	instances = append(instances, firstInstance)

	// Start all other instances concurrently, joining the 1st instance.
	var nextInstances []e2e.Service
	for i := 2; i <= maxMimir; i++ {
		name := fmt.Sprintf("mimir-%d", i)
		join := e2e.NetworkContainerHostPort(networkName, firstInstance.Name(), 8000)
		c := newSingleBinary(name, "", join, nil)
		nextInstances = append(nextInstances, c)
		instances = append(instances, c)
	}
	require.NoError(t, s.StartAndWaitReady(nextInstances...))

	// Sanity check the ring membership and give each instance time to see every other instance.

	for _, c := range instances {
		// we expect 3*maxMimir to account for both the ingester, distributor and compactor rings
		require.NoError(t, c.WaitSumMetrics(e2e.Equals(float64(maxMimir*3)), "cortex_ring_members"))
		require.NoError(t, c.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
	}

	// Scale down as fast as possible but cleanly, in order to send out tombstones.

	stop := errgroup.Group{}
	for len(instances) > minMimir {
		i := len(instances) - 1
		c := instances[i]
		instances = instances[:i]
		stop.Go(func() error { return s.Stop(c) })

		// TODO(#4360): Remove this when issue is resolved.
		//   Wait until memberlist for all nodes has recognised the instance left.
		//   This means that we will not gossip tombstones to leaving nodes.
		for _, c := range instances {
			require.NoError(t, c.WaitSumMetrics(e2e.Equals(float64(len(instances))), "memberlist_client_cluster_members_count"))
		}
	}
	require.NoError(t, stop.Wait())

	// If all is working as expected, then tombstones should have propagated easily within this time period.
	// The logging is mildly spammy, but it has proven extremely useful for debugging convergence cases.
	// We don't use WaitSumMetrics [over all instances] here so we can log the per-instance metrics.

	// These values are multiplied by three to account for the fact that ingester
	// distributor and compactor components use a ring.
	expectedRingMembers := float64(minMimir) * 3
	expectedTombstones := float64(maxMimir-minMimir) * 3

	require.Eventually(t, func() bool {
		ok := true
		for _, c := range instances {
			metrics, err := c.SumMetrics([]string{
				"cortex_ring_members", "memberlist_client_kv_store_value_tombstones",
			})
			require.NoError(t, err)
			t.Logf("%s: cortex_ring_members=%f memberlist_client_kv_store_value_tombstones=%f\n",
				c.Name(), metrics[0], metrics[1])

			// Don't short circuit the check, so we log the state for all instances.
			if metrics[0] != expectedRingMembers {
				ok = false
			}
			if metrics[1] != expectedTombstones {
				ok = false
			}

		}
		return ok
	}, 30*time.Second, 2*time.Second,
		"expected all instances to have %f ring members and %f tombstones",
		expectedRingMembers, expectedTombstones)
}
