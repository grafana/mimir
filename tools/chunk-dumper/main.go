// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/mitchellh/go-homedir"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

func main() {
	a := &app{}
	if err := a.run(); err != nil {
		slog.Error("application exited with error", "err", err)
		os.Exit(-1)
	}
}

type app struct {
	kubeContext   string
	kubeNamespace string
	podNames      flagext.StringSlice

	targetPort uint

	startT       flagext.Time
	endT         flagext.Time
	parsedStartT time.Time
	parsedEndT   time.Time

	matchers       string
	parsedMatchers []*labels.Matcher

	kubeRoundTripper  http.RoundTripper
	kubeUpgrader      spdy.Upgrader
	kubeBaseTargetURL *url.URL
}

func (a *app) run() error {
	if err := a.parseArgs(); err != nil {
		return err
	}

	if err := a.loadKubeConfig(); err != nil {
		return err
	}

	g := errgroup.Group{}

	for _, podName := range a.podNames {
		podName := podName
		g.Go(func() error {
			return a.runForPod(podName)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	slog.Info("done!")
	return nil
}

func (a *app) parseArgs() error {
	flag.StringVar(&a.kubeContext, "kube-context", "", "Kubernetes CLI context to use")
	flag.StringVar(&a.kubeNamespace, "kube-namespace", "", "Kubernetes namespace to use")
	flag.Var(&a.podNames, "pod", "Pod names to query")
	flag.UintVar(&a.targetPort, "target-port", 9095, "Port on pods to connect to")
	flag.Var(&a.startT, "start-time", "Start of time range to query for")
	flag.Var(&a.endT, "end-time", "End of time range to query for")
	flag.StringVar(&a.matchers, "matchers", "", `Matchers to query for, in PromQL format (eg. 'my_metric{namespace=~"(dev|test)"}')`)
	flag.Parse()

	if a.kubeContext == "" {
		return errors.New("must provide Kubernetes CLI context")
	}

	if a.kubeNamespace == "" {
		return errors.New("must provide Kubernetes namespace")
	}

	if len(a.podNames) == 0 {
		return errors.New("must provide at least one Kubernetes pod name")
	}

	if a.targetPort == 0 {
		return errors.New("must provide a target port to connect to")
	}

	a.parsedStartT = time.Time(a.startT)
	a.parsedEndT = time.Time(a.endT)

	if a.parsedStartT.IsZero() {
		return errors.New("must provide start time")
	}

	if a.parsedEndT.IsZero() {
		return errors.New("must provide end time")
	}

	if a.parsedStartT.Equal(a.parsedEndT) || a.parsedStartT.After(a.parsedEndT) {
		return errors.New("start time must be before end time")
	}

	if a.matchers == "" {
		return errors.New("must provide matchers")
	}

	matchers, err := parser.ParseMetricSelector(a.matchers)
	if err != nil {
		return fmt.Errorf("invalid matchers '%v': %w", a.matchers, err)
	}

	a.parsedMatchers = matchers

	return nil
}

func (a *app) runForPod(podName string) error {
	localPort, cleanup, err := a.establishPortForward(podName)
	if err != nil {
		return err
	}
	defer cleanup()

	return a.queryPod(podName, localPort)
}

func (a *app) queryPod(podName string, localPort uint16) error {
	slog.Info("querying pod", "name", podName, "local_port", localPort)
	return nil
}

func (a *app) loadKubeConfig() error {
	homeDir, err := homedir.Dir()
	if err != nil {
		return fmt.Errorf("could not determine home directory: %w", err)
	}

	kubeConfigPath := filepath.Join(homeDir, ".kube", "config")
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigPath}, &clientcmd.ConfigOverrides{CurrentContext: a.kubeContext}).ClientConfig()
	if err != nil {
		return fmt.Errorf("could not create Kubernetes client configuration: %w", err)
	}

	a.kubeRoundTripper, a.kubeUpgrader, err = spdy.RoundTripperFor(config)
	if err != nil {
		return fmt.Errorf("could not create round tripper: %w", err)
	}

	a.kubeBaseTargetURL, err = url.Parse(config.Host)
	if err != nil {
		return fmt.Errorf("could not parse Kubernetes API host URL: %w", err)
	}

	return nil
}

func (a *app) establishPortForward(podName string) (uint16, func(), error) {
	forwarder, readyChan, stopChan, err := a.startPortForward(podName)
	if err != nil {
		return 0, nil, err
	}

	finishedChan := make(chan struct{}, 1)
	var forwardingErr error

	cleanup := func() {
		close(stopChan)
		<-finishedChan

		if err == nil && forwardingErr != nil {
			err = forwardingErr
		}
	}

	go func() {
		slog.Info("establishing port forward to pod", "name", podName)
		forwardingErr = forwarder.ForwardPorts()
		close(finishedChan)
	}()

	select {
	case <-readyChan:
		// Port forwarding is up and running.
	case <-finishedChan:
		cleanup()
		return 0, nil, fmt.Errorf("port forwarding could not start: %w", forwardingErr)
	}

	ports, err := forwarder.GetPorts()
	if err != nil {
		cleanup()
		return 0, nil, fmt.Errorf("could not get local port: %w", err)
	}

	localPort := ports[0].Local

	return localPort, cleanup, nil
}

func (a *app) startPortForward(podName string) (*portforward.PortForwarder, chan struct{}, chan struct{}, error) {
	targetURL := a.kubeBaseTargetURL.JoinPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", a.kubeNamespace, podName))
	dialer := spdy.NewDialer(a.kubeUpgrader, &http.Client{Transport: a.kubeRoundTripper}, http.MethodPost, targetURL)

	stopChan := make(chan struct{}, 1)
	readyChan := make(chan struct{}, 1)
	out := bytes.Buffer{}
	errOut := bytes.Buffer{}
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf(":%v", a.targetPort)}, stopChan, readyChan, &out, &errOut)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not create port forwarder: %w", err)
	}

	return forwarder, readyChan, stopChan, nil
}
