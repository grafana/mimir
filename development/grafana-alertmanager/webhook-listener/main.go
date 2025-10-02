package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/common/model"
)

var (
	resultsOutputPath = "/shared"
	containerName     = "wh-1"
)

func init() {
	defaultContainer := containerName

	hn, err := os.Hostname()
	if err != nil {
		log.Printf("failed to get hostname: %v", err)
	} else {
		defaultContainer = hn
	}

	flag.StringVar(&resultsOutputPath, "results-output-path", "/shared", "Path where to save the results at")
	flag.StringVar(&containerName, "container", defaultContainer, "Name used to identify a specific webhook instance")

	flag.Parse()

	log.Printf(`Parsed flags:
-------
	resultsOutputPath: %q
	containerName: %q
`, resultsOutputPath, containerName)
}

type Event struct {
	Alerts            int               `json:"alerts"`
	Fingerprints      map[string]string `json:"fingerprints"`
	Status            string            `json:"status"`
	GroupKey          string            `json:"groupKey"`
	GroupLabels       model.LabelSet    `json:"groupLabels"`
	GroupFingerprint  string            `json:"groupFingerprint"`
	TimeNow           time.Time         `json:"timeNow"`
	Node              string            `json:"node"`
	DeltaLastSeconds  float64           `json:"deltaLastSeconds"`
	DeltaStartSeconds float64           `json:"deltaStartSeconds"`
}

type Notification struct {
	Alerts            []Alert           `json:"alerts"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	CommonLabels      map[string]string `json:"commonLabels"`
	ExternalURL       string            `json:"externalURL"`
	GroupKey          string            `json:"groupKey"`
	GroupLabels       model.LabelSet    `json:"groupLabels"`
	Message           string            `json:"message"`
	OrgID             int               `json:"orgId"`
	Receiver          string            `json:"receiver"`
	State             string            `json:"state"`
	Status            string            `json:"status"`
	Title             string            `json:"title"`
	TruncatedAlerts   int               `json:"truncatedAlerts"`
	Version           string            `json:"version"`
}

type Alert struct {
	Annotations  map[string]string `json:"annotations"`
	DashboardURL string            `json:"dashboardURL"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	Fingerprint  string            `json:"fingerprint"`
	GeneratorURL string            `json:"generatorURL"`
	Labels       map[string]string `json:"labels"`
	PanelURL     string            `json:"panelURL"`
	SilenceURL   string            `json:"silenceURL"`
	Status       string            `json:"status"`
	ValueString  string            `json:"valueString"`
	Values       map[string]any    `json:"values"`
}

type NotificationHandler struct {
	startedAt time.Time
	stats     map[string]int
	hist      []Event
	m         sync.RWMutex
}

func NewNotificationHandler() *NotificationHandler {
	return &NotificationHandler{
		startedAt: time.Now(),
		stats:     make(map[string]int),
		hist:      make([]Event, 0),
	}
}

func (ah *NotificationHandler) Notify(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n := Notification{}
	if err := json.Unmarshal(b, &n); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("got notification from: %s. a: %v", r.RemoteAddr, n)

	ah.m.Lock()
	defer ah.m.Unlock()

	addr := r.RemoteAddr
	if split := strings.Split(r.RemoteAddr, ":"); len(split) > 0 {
		addr = split[0]
	}

	fps := make(map[string]string, len(n.Alerts))
	for _, a := range n.Alerts {
		id := a.Labels["id"]
		fps[id] = a.Fingerprint
	}

	timeNow := time.Now()

	ah.stats[n.Status]++

	var d time.Duration
	if len(ah.hist) > 0 {
		last := ah.hist[len(ah.hist)-1]
		d = timeNow.Sub(last.TimeNow)
	}

	ah.hist = append(ah.hist, Event{
		Alerts:            len(n.Alerts),
		Status:            n.Status,
		Fingerprints:      fps,
		GroupKey:          n.GroupKey,
		GroupLabels:       n.GroupLabels,
		GroupFingerprint:  n.GroupLabels.Fingerprint().String(),
		TimeNow:           timeNow,
		Node:              addr,
		DeltaLastSeconds:  d.Seconds(),
		DeltaStartSeconds: timeNow.Sub(ah.startedAt).Seconds(),
	})
}

func (ah *NotificationHandler) GetNotifications(w http.ResponseWriter, _ *http.Request) {
	ah.m.RLock()
	defer ah.m.RUnlock()
	w.Header().Set("Content-Type", "application/json")

	res, err := json.MarshalIndent(map[string]any{"stats": ah.stats, "history": ah.hist}, "", "\t")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		//nolint:errcheck
		w.Write([]byte(`{"error":"failed to marshal alerts"}`))
		log.Printf("failed to marshal alerts: %v\n", err)
		return
	}

	log.Printf("requested current state\n%v\n", string(res))

	_, err = w.Write(res)
	if err != nil {
		log.Printf("failed to write response: %v\n", err)
	}
}

func main() {
	ah := NewNotificationHandler()

	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/notify", ah.Notify)
	http.HandleFunc("/notifications", ah.GetNotifications)

	server := &http.Server{
		Addr: "0.0.0.0:8080",
	}

	// Channel to listen for interrupt or terminate signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Run server in a goroutine
	go func() {
		log.Println("Listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v\n", err)
		}
	}()

	// Block until we receive a signal
	<-stop
	log.Println("Shutdown signal received")

	// Create a deadline for the shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v\n", err)
	}

	log.Println("Server stopped")
	if err := ah.dumpState(); err != nil {
		log.Printf("Failed to dump state: %v\n", err)
	}
}

func (ah *NotificationHandler) dumpState() error {
	log.Printf("dumping results to folder %q", resultsOutputPath)

	ah.m.RLock()
	defer ah.m.RUnlock()

	state := map[string]any{
		"stats":   ah.stats,
		"history": ah.hist,
	}

	sb, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(resultsOutputPath, fmt.Sprintf("%s.json", containerName)), sb, 0644)
	if err != nil {
		return err
	}

	return nil
}
