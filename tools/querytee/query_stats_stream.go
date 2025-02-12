package querytee

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-logfmt/logfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type LogEntry struct {
	Path      string                 `json:"path"`
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	User      string                 `json:"user"`
	Message   string                 `json:"message"`
	Params    map[string]interface{} `json:"params"`
}

type queryStatsStreamerConfig struct {
	K8sNamespace         string
	K8sPodSelectors      string
	QueryFilter          string
	MaxParallelism       int
	MaxRequestsPerMinute int
}

func RegisterQueryStatsStreamerFlags(f *flag.FlagSet) *queryStatsStreamerConfig {
	cfg := &queryStatsStreamerConfig{}
	f.StringVar(&cfg.K8sNamespace, "query-stats-streaming.k8s-namespace", "", "Kubernetes namespace to use for querying logs")
	f.StringVar(&cfg.K8sPodSelectors, "query-stats-streaming.k8s-pod-selectors", "", "Comma-separated Kubernetes pod selectors to use for querying logs")
	f.StringVar(&cfg.QueryFilter, "query-stats-streaming.query-filter", "", "Regular expression to filter queries")
	f.IntVar(&cfg.MaxParallelism, "query-stats-streaming.max-parallelism", 50, "Maximum number of concurrent HTTP requests")
	f.IntVar(&cfg.MaxRequestsPerMinute, "query-stats-streaming.max-requests-per-minute", 30, "Maximum number of queries per second")

	return cfg
}

type queryStatsStreamer struct {
	// Metrics
	metricRequestsTotal    prometheus.Counter
	metricActiveLogStreams prometheus.Gauge
	metricInflightRequests prometheus.Gauge

	cfg    *queryStatsStreamerConfig
	logger log.Logger

	clientset         *kubernetes.Clientset
	queryFilterRegexp *regexp.Regexp                // Compiled query filter regexp
	podStreams        map[string]context.CancelFunc // Tracks active pod log streams
	podStreamsMu      sync.Mutex                    // Protects podStreams map
	client            *http.Client                  // HTTP client for forwarding queries
	forwardBaseAddr   string
	parallelismSem    chan struct{}
	requestWaitSem    chan struct{}
}

func newQueryStatsStreamer(cfg *queryStatsStreamerConfig, logger log.Logger, reg prometheus.Registerer, forwardBaseAddr string) *queryStatsStreamer {
	streamer := &queryStatsStreamer{
		metricRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "query_stats_streamer_requests_total",
			Help: "Total number of forwarded queries",
		}),
		metricActiveLogStreams: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "query_stats_streamer_active_log_streams",
			Help: "Number of active log streams",
		}),
		metricInflightRequests: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "query_stats_streamer_inflight_requests",
			Help: "Number of inflight requests",
		}),
		cfg:        cfg,
		logger:     logger,
		podStreams: make(map[string]context.CancelFunc),
		client: &http.Client{
			Timeout: 2 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        cfg.MaxParallelism,
				MaxIdleConnsPerHost: cfg.MaxParallelism,
				MaxConnsPerHost:     cfg.MaxParallelism,
			},
		},
		parallelismSem:  make(chan struct{}, cfg.MaxParallelism),
		requestWaitSem:  make(chan struct{}, cfg.MaxRequestsPerMinute),
		forwardBaseAddr: forwardBaseAddr,
	}

	streamer.queryFilterRegexp = regexp.MustCompile(cfg.QueryFilter)
	return streamer
}

func (s *queryStatsStreamer) Run() error {
	// Pre-load the request per minute semaphore (to avoid always doing requests in a burst)
	level.Info(s.logger).Log("msg", "Blocking the request semaphore while loading")
	for i := 0; i < s.cfg.MaxRequestsPerMinute; i++ {
		s.requestWaitSem <- struct{}{}
	}

	// Unload the semaphore after a minute, spreading requests over a minute
	go func() {
		level.Info(s.logger).Log("msg", "Waiting for log streams to start")
		for len(s.podStreams) == 0 {
			time.Sleep(1 * time.Second)
		}

		level.Info(s.logger).Log("msg", "Unloading request semaphore")
		for i := 0; i < s.cfg.MaxRequestsPerMinute; i++ {
			time.Sleep(time.Minute / time.Duration(s.cfg.MaxRequestsPerMinute))
			<-s.requestWaitSem
		}
		level.Info(s.logger).Log("msg", "Done unloading request semaphore. Requests will now be limited to the configured rate.")
	}()

	// Kubernetes client configuration
	var config *rest.Config
	var err error

	// Try in-cluster config first
	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig if in-cluster config fails
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("error getting user home directory: %v", err)
		}
		kubeconfig := fmt.Sprintf("%s/.kube/config", home)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return fmt.Errorf("error creating Kubernetes client config: %v", err)
		}
	}

	s.clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %v", err)
	}

	// Start the pod watcher
	go s.watchPods()

	// Keep the main function running
	select {}
}

// watchPods periodically checks for new or deleted pods and manages log streams
func (s *queryStatsStreamer) watchPods() {
	ticker := time.NewTicker(1 * time.Minute) // Adjust the interval as needed
	defer ticker.Stop()

	for range ticker.C {
		pods := []string{}

		selectors := strings.Split(s.cfg.K8sPodSelectors, ",")
		for _, selector := range selectors {
			deploymentPods, err := s.clientset.CoreV1().Pods(s.cfg.K8sNamespace).List(context.TODO(), metav1.ListOptions{
				LabelSelector:  selector,
				TimeoutSeconds: new(int64), // No timeout
			})
			if err != nil {
				level.Error(s.logger).Log("msg", "Error listing pods", "err", err)
				continue
			}

			for _, pod := range deploymentPods.Items {
				pods = append(pods, pod.Name)
			}
		}

		// Track current pods
		currentPods := make(map[string]bool)
		for _, podName := range pods {
			currentPods[podName] = true
			if _, exists := s.podStreams[podName]; !exists {
				// Start a new log stream for this pod
				ctx, cancel := context.WithCancel(context.Background())
				s.podStreamsMu.Lock()
				s.podStreams[podName] = cancel
				s.podStreamsMu.Unlock()
				go func() {
					defer cancel()
					s.streamPodLogs(ctx, podName)
					level.Info(s.logger).Log("msg", "Stopped log stream for pod", "pod", podName)
					s.podStreamsMu.Lock()
					delete(s.podStreams, podName)
					s.podStreamsMu.Unlock()
				}()
			}
		}

		// Clean up deleted pods
		s.podStreamsMu.Lock()
		for podName, cancel := range s.podStreams {
			if !currentPods[podName] {
				cancel() // Stop the log stream
				delete(s.podStreams, podName)
				level.Info(s.logger).Log("msg", "Stopped log stream for deleted pod", "pod", podName)
			}
		}
		s.podStreamsMu.Unlock()
	}
}

// streamPodLogs streams logs from a specific pod
func (s *queryStatsStreamer) streamPodLogs(ctx context.Context, podName string) {
	s.metricActiveLogStreams.Inc()
	defer s.metricActiveLogStreams.Dec()
	level.Info(s.logger).Log("msg", "Starting log stream for pod", "pod", podName)

	req := s.clientset.CoreV1().Pods(s.cfg.K8sNamespace).GetLogs(podName, &corev1.PodLogOptions{
		Follow:    true,
		SinceTime: &metav1.Time{Time: time.Now()},
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		level.Error(s.logger).Log("msg", "Error opening log stream", "pod", podName, "err", err)
		return
	}
	defer stream.Close()

	decoder := logfmt.NewDecoder(stream)
	for {
		select {
		case <-ctx.Done():
			return // Stop streaming if the context is canceled
		default:
			if !decoder.ScanRecord() {
				if decoder.Err() != nil {
					level.Error(s.logger).Log("msg", "Error decoding logfmt record", "pod", podName, "err", decoder.Err())
				}
				return
			}

			entry := LogEntry{
				Params: make(map[string]interface{}),
			}

			skip := false
			for decoder.ScanKeyval() {
				key := string(decoder.Key())
				value := string(decoder.Value())

				switch key {
				case "path":
					entry.Path = value
				case "ts":
					entry.Timestamp = value
				case "level":
					entry.Level = value
				case "user":
					entry.User = value
				case "msg":
					entry.Message = value
				default:
					if len(key) > 6 && key[:6] == "param_" {
						paramKey := key[6:]
						entry.Params[paramKey] = value

						if s.queryFilterRegexp != nil && paramKey == "query" && !s.queryFilterRegexp.MatchString(value) {
							skip = true
						}
					}
				}
			}

			if entry.Message == "query stats" && entry.Path == "/prometheus/api/v1/query" && !skip {
				// Forward the query if under the maximum RPS
				// To do that take a spot in the semaphore and release it after a second
				// If maxParallelism is reached, block until a slot is available
				s.requestWaitSem <- struct{}{}
				s.parallelismSem <- struct{}{}
				// Wait a minute and release the RPS limit slot
				go func() {
					defer func() { <-s.requestWaitSem }()
					time.Sleep(time.Minute)
				}()
				go func() {
					// Release connection slot when request is done
					defer func() { <-s.parallelismSem }()
					s.forwardQuery(entry)
				}()
			}
		}
	}
}

// forwardLog forwards a query
func (s *queryStatsStreamer) forwardQuery(entry LogEntry) {
	s.metricRequestsTotal.Inc()
	s.metricInflightRequests.Inc()
	defer s.metricInflightRequests.Dec()

	q := url.Values{}
	for key, value := range entry.Params {
		q.Add(key, value.(string))
	}
	reqURL := &url.URL{
		Scheme:   "http",
		Host:     s.forwardBaseAddr,
		Path:     entry.Path,
		RawQuery: q.Encode(),
	}

	req, err := http.NewRequest("POST", reqURL.String(), http.NoBody)
	if err != nil {
		level.Error(s.logger).Log("msg", "Error creating HTTP request", "err", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if entry.User != "" {
		req.Header.Set("X-Scope-OrgId", entry.User)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		level.Error(s.logger).Log("msg", "Error forwarding log", "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		level.Warn(s.logger).Log("msg", "Received non-OK response", "status", resp.Status, "body", string(body))
	}

	// Read the entire response body to reuse the connection
	io.Copy(io.Discard, resp.Body)
}

// Pod is a collection of containers that can run on a host. This resource is created
// by clients and scheduled onto hosts.
type Pod struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodList is a list of Pods.
type PodList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of pods.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md
	Items []Pod `json:"items" protobuf:"bytes,2,rep,name=items"`
}
