// SPDX-License-Identifier: AGPL-3.0-only

package limits

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"
)

var (
	RuntimeConfigSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_runtime_config_last_reload_successful",
		Help: "Whether the last runtime-config reload attempt was successful.",
	})
	RuntimeConfigHash = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_runtime_config_hash",
		Help: "Hash of the currently active runtime configuration, merged from all configured files.",
	}, []string{"sha256"})
	RuntimeConfigHTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "cortex_runtime_config_http_request_duration_seconds",
		Help: "Time spent fetching runtime config from HTTP endpoints.",
	}, []string{"url", "status_code"})
)

// Collectors are this package's metrics, for the ingester's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{RuntimeConfigSuccess, RuntimeConfigHash, RuntimeConfigHTTPDuration}
}

// RuntimeConfigArgs configure Mimir's runtime config (`-runtime-config.file`).
type RuntimeConfigArgs struct {
	// Comma-separated YAML or JSON files and HTTP URLs, merged from left to right.
	File              string
	ReloadPeriod      string
	HTTPClientTimeout string
	// Sent as `X-Cluster` when fetching URLs, for servers that validate the cluster label.
	ClusterValidationLabel string
}

func (a *RuntimeConfigArgs) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&a.File, "runtime-config.file", "", "Comma-separated YAML or JSON files and HTTP URLs, merged from left to right.")
	f.StringVar(&a.ReloadPeriod, "runtime-config.reload-period", "10s", "")
	f.StringVar(&a.HTTPClientTimeout, "runtime-config.http-client-timeout", "30s", "")
	f.StringVar(&a.ClusterValidationLabel, "common.client-cluster-validation.label", "", "")
}

// RuntimeConfig loads a comma-separated list of YAML or JSON files and HTTP URLs, merged left to
// right like dskit's runtimeconfig manager, and reloaded periodically.
type RuntimeConfig struct {
	sources      []string
	timeout      time.Duration
	clusterLabel string
	client       *http.Client
	last         []byte
	hasLast      bool
}

func NewRuntimeConfig(args RuntimeConfigArgs) (*RuntimeConfig, error) {
	var sources []string
	for _, source := range strings.Split(args.File, ",") {
		if source = strings.TrimSpace(source); source != "" {
			sources = append(sources, source)
		}
	}
	timeoutMs, err := ParseDurationMs(args.HTTPClientTimeout)
	if err != nil {
		return nil, err
	}
	return &RuntimeConfig{
		sources:      sources,
		timeout:      time.Duration(timeoutMs) * time.Millisecond,
		clusterLabel: args.ClusterValidationLabel,
		client:       &http.Client{},
	}, nil
}

func (c *RuntimeConfig) IsEmpty() bool { return len(c.sources) == 0 }

func (c *RuntimeConfig) read(ctx context.Context, source string) ([]byte, error) {
	if strings.HasPrefix(source, "https://") {
		return nil, errors.New("https runtime config URLs are not supported")
	}
	if !strings.HasPrefix(source, "http://") {
		data, err := os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", source, err)
		}
		return data, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", "dskit-runtimeconfig")
	if c.clusterLabel != "" {
		request.Header.Set("X-Cluster", c.clusterLabel)
	}
	started := time.Now()
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	RuntimeConfigHTTPDuration.WithLabelValues(source, strconv.Itoa(response.StatusCode)).Observe(time.Since(started).Seconds())
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, fmt.Errorf("%s returned %s", source, response.Status)
	}
	return body, nil
}

// Load reads every source and applies the merged config if any changed.
func (c *RuntimeConfig) Load(ctx context.Context, overrides *Overrides) (bool, error) {
	raw := make([][]byte, 0, len(c.sources))
	for _, source := range c.sources {
		data, err := c.read(ctx, source)
		if err != nil {
			return false, fmt.Errorf("read %q: %w", source, err)
		}
		raw = append(raw, data)
	}
	fingerprint := bytes.Join(raw, []byte{0})
	if c.hasLast && bytes.Equal(c.last, fingerprint) {
		return false, nil
	}
	merged := map[string]any{}
	for i, source := range c.sources {
		document, err := ParseDocument(raw[i])
		if err != nil {
			return false, fmt.Errorf("unmarshal %q: %w", source, err)
		}
		if merged, err = MergeMaps(merged, document, ""); err != nil {
			return false, fmt.Errorf("can't merge %q on top of the previous providers: %w", source, err)
		}
	}
	if err := overrides.ApplyRuntimeConfig(merged); err != nil {
		return false, err
	}
	setRuntimeConfigHash(fingerprintHash(fingerprint))
	c.last, c.hasLast = fingerprint, true
	return true, nil
}

// Run reloads until ctx is done; like dskit, failures keep the previous config.
func (c *RuntimeConfig) Run(ctx context.Context, overrides *Overrides, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		changed, err := c.Load(ctx, overrides)
		if err != nil {
			RuntimeConfigSuccess.Set(0)
			log.Printf("phase=runtime_config_error error=%v", err)
			continue
		}
		RuntimeConfigSuccess.Set(1)
		if changed {
			log.Printf("phase=runtime_config_reloaded")
		}
	}
}

func setRuntimeConfigHash(hash uint64) {
	RuntimeConfigHash.Reset()
	RuntimeConfigHash.WithLabelValues(fmt.Sprintf("%016x", hash)).Set(1)
	RuntimeConfigSuccess.Set(1)
}

// fingerprintHash stands in for dskit's SHA-256 in the `cortex_runtime_config_hash` label; only
// changes matter.
func fingerprintHash(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// ParseDocument parses a runtime config file, JSON or YAML, into a map with string keys.
func ParseDocument(data []byte) (map[string]any, error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	var value any
	switch {
	case len(trimmed) == 0:
		return map[string]any{}, nil
	case trimmed[0] == '{':
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			if err := yaml.Unmarshal(data, &value); err != nil {
				return nil, err
			}
		}
	default:
		if err := yaml.Unmarshal(data, &value); err != nil {
			return nil, err
		}
	}
	switch value := Normalize(value).(type) {
	case map[string]any:
		return value, nil
	case nil:
		return map[string]any{}, nil
	default:
		return nil, fmt.Errorf("runtime config must be a map, got %v", value)
	}
}

// Normalize turns YAML maps with non-string keys, like numeric tenant IDs, into maps with string
// keys, recursively.
func Normalize(value any) any {
	switch value := value.(type) {
	case map[string]any:
		for key, v := range value {
			value[key] = Normalize(v)
		}
		return value
	case map[any]any:
		out := make(map[string]any, len(value))
		for key, v := range value {
			out[fmt.Sprint(key)] = Normalize(v)
		}
		return out
	case []any:
		for i, v := range value {
			value[i] = Normalize(v)
		}
		return value
	default:
		return value
	}
}

// MergeMaps deep-merges b over a like dskit's `mergeConfigMaps`: maps merge key by key, anything
// else is replaced, and a null on either side of a map counts as an empty map.
func MergeMaps(a, b map[string]any, path string) (map[string]any, error) {
	out := a
	for key, value := range b {
		keyPath := path + "." + key
		existing, exists := out[key]
		left, leftMap := existing.(map[string]any)
		right, rightMap := value.(map[string]any)
		var merged any
		switch {
		case exists && leftMap && rightMap:
			var err error
			if merged, err = MergeMaps(left, right, keyPath); err != nil {
				return nil, err
			}
		case exists && leftMap && value == nil:
			merged = left
		case exists && existing == nil && rightMap:
			merged = right
		case exists && leftMap:
			return nil, fmt.Errorf("conflicting types for %q: map != %v", keyPath, value)
		case exists && existing != nil && rightMap:
			return nil, fmt.Errorf("conflicting types for %q: %v != map", keyPath, existing)
		default:
			merged = value
		}
		out[key] = merged
	}
	return out, nil
}
