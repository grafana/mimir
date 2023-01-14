---
title: Get started with Prometheus and Grafana from scratch
menuTitle: Get started with Prometheus and Grafana from scratch
description: Create a Go app, create custom metrics, 
aliases:
  - /docs/sources/mimir/tutorials/get-started-with-prometheus-from-scratch
weight: 100
keywords:
  - Prometheus
  - Grafana
  - Go
  - metrics
  - counter
  - gauge
  - histogram
---

# Get Started with Prometheus and Grafana from Scratch

Prometheus is a powerful tool for collecting time series metrics and pairs very well with Grafana to visualize data from services that run 24/7. What does all this look like under the hood though? 

There are many ways to create a project similar to this, many ways of setting up custom metrics, and many flavors of Prometheus to choose from. With all of the options available, it can be easy for a person to become overwhelmed with decision fatigue. This tutorial is meant to allow a user see the process from start to finish. We describe how to do the following: 

- Set up a Go application 
- Configure the app to send custom metrics 
- Configure a telemetry collector via Grafana Agent to send metrics to a Grafana Prometheus instance 
- Visualize metrics using Grafana

[Include miro flow chart image here]

## Before you begin

- [Sign up for Grafana Cloud](https://grafana.com/auth/sign-up/create-user)
- Go 1.19

## Set up a Go application

To [set up a Go application]:

1. Create a directory for the application and name it `observe-go`. 

1. Enter the directory and initialize a go.mod file.

`go mod init example.com/observe-go`

1. Install `prometheus`, `promauto`, and `promhttp` using `go get`.

```
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promauto
go get github.com/prometheus/client_golang/prometheus/promhttp
```

1. Create a file for the Go code which we will call `main.go`. Here we will do the following:

- Create a server
- Expose a metrics endpoint  
- create custom metrics. 

In `main.go`, add the following code. This is a variation of the code found in the [Prometheus docs](https://prometheus.io/docs/guides/go-application/#instrumenting-a-go-application-for-prometheus).

```
package main
 
import (
 "github.com/prometheus/client_golang/prometheus/promhttp"
 "log"
 "net/http"
)
 
func main() {
 http.Handle("/metrics", promhttp.Handler())
 log.Fatal(http.ListenAndServe(":8080", nil))
}
```

1. Start the application.

`go run main.go`

1. Confirm metrics by navigating to the following URL.

`http://localhost:8080/metrics`

These metrics are provided out of the box with Prometheus. Next we will create custom metrics to get more insight from our application and explore various capabilities of Prometheus. 

## Create Custom Metrics

Create a counter, a gauge, and histogram metrics.

### Counter

Counters can only go up (and reset, such as when a process restarts). They are useful for accumulating the number of events, or the amount of something at each event. For example, the total number of HTTP requests, or the total number of bytes sent in HTTP requests. Raw counters are rarely useful. Use the rate() function to get the per-second rate at which they are increasing.

1. In `main.go` instantiate the counter with `NewCounter` and pass this to the `CounterOpts`.

```
var (
 counter = promauto.NewCounter(prometheus.CounterOpts{
   Name: "observe_go_counter",
   Help: "The counter increments randomly between 1 and 5 seconds",
 })
)
```

1. Create a function that increments the counter randomly.

```
func recordCounter() {
 go func() {
   for {
     counter.Inc()
     randomTime := time.Duration(rand.Intn(5))
     time.Sleep(randomTime * time.Second)
   }
 }()
}
```

1. Call the counter in the main function.

```
func main() {
 recordCounter()
 
 http.Handle("/metrics", promhttp.Handler())
 log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Gauge
 
Gauges also represent a single numerical value but different to counters the value can go up as well as down. Therefore gauges are often used for measured values like temperature, humidity or current memory usage.

1. Instantiate a new gauge with `GaugeOpts`.

```
var (
 gauge = promauto.NewGauge(prometheus.GaugeOpts{
   Name: "observe_go_gauge",
   Help: "The gauge starts at 0 and randomly goes up or down",
 })
)
```

1. Create a function to increment and decrement the counter randomly.

```
func updategauge() {
 go func() {
   for {
     if rand.Intn(10)%2 == 0 {
       gauge.Inc()
     } else {
       gauge.Dec()
     }
 
     time.Sleep(1 * time.Second)
   }
 }() 
}
```

1. Call the gauge function in the main function.

```
func main() {
 recordCounter()
 updategauge()
 
 http.Handle("/metrics", promhttp.Handler())
 log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Histogram

A histogram counts individual observations from an event or sample stream in configurable static buckets (or in dynamic sparse buckets as part of the experimental Native Histograms, see below for more details). Similar to a Summary, it also provides a sum of observations and an observation count.

1. Instantiate a histogram with HistogramOpts.

```
var (
 histogram = prometheus.NewHistogram(prometheus.HistogramOpts{
   Name:    "observe_go_histogram",
   Help:    "A histogram",
   Buckets: prometheus.LinearBuckets(5, 10, 5),
 })
)
```

Notice that we define the buckets in the HistogramOpts. `LinearBuckets` takes three arguments,`start`, `width` and `count`. LinearBuckets creates `count` regular buckets, each `width` wide, where the lowest bucket has an upper bound of `start`. 

In this example we are creating 5 buckets, where each bucket is 10 elements wide. The first bucket is 0-5, the second is 6-15, the third is 16-25, the fourth is 26-35 and the fifth bucket is 36-45. The label “le” means “less than or equal to.” 

```
observe_go_histogram_bucket{le="5"} 
observe_go_histogram_bucket{le="15"} 
observe_go_histogram_bucket{le="25"} 
observe_go_histogram_bucket{le="35"} 
observe_go_histogram_bucket{le="45"} 
observe_go_histogram_bucket{le="+Inf"}
```

1. Create a function to  `Observe` a value every second.This random value will be placed in the correct bucket and that bucket will be incremented.

```
func updateHistogram() {
 go func() {
   for {
     randomValue := rand.Intn(50)
     histogram.Observe(float64(randomValue))
     time.Sleep(1 * time.Second)
   }
 }()
}
```

1. Register the histogram function and call it in the main function.

```
func main() {
 // register the histogram
 prometheus.Register(histogram)
 
 recordCounter()
 updategauge()
 updateHistogram()
 
 http.Handle("/metrics", promhttp.Handler())
 log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The entire contents of main should now look like the following:

```
package main

import (
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	counter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "observe_go_counter",
		Help: "The counter increments randomly between 1 and 5 seconds",
	})
)

func recordCounter() {
	go func() {
		for {
			counter.Inc()
			randomTime := time.Duration(rand.Intn(5))
			time.Sleep(randomTime * time.Second)
		}
	}()
}

var (
	gauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "observe_go_gauge",
		Help: "The gauge starts at 0 and randomly goes up or down",
	})
)

func updategauge() {
	go func() {
		for {
			if rand.Intn(10)%2 == 0 {
				gauge.Inc()
			} else {
				gauge.Dec()
			}

			time.Sleep(1 * time.Second)
		}
	}()
}

var (
	histogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "observe_go_histogram",
		Help:    "A histogram",
		Buckets: prometheus.LinearBuckets(5, 10, 5),
	})
)

func updateHistogram() {
	go func() {
		for {
			randomValue := rand.Intn(45)
			histogram.Observe(float64(randomValue))
			time.Sleep(1 * time.Second)
		}
	}()
}

func main() {
	// must register the histogram
	prometheus.Register(histogram)

	recordCounter()
	updategauge()
	updateHistogram()

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

1. Confirm the custom metrics by navigating to http://localhost:8080/metrics 

1. Search for `observe_go_counter`, `observe_go_gauge`, and `observe_go_histogram` in the list of metrics.

## Use the Grafana Agent to Send Metrics to Grafana

It is incredibly easy to get started with a Prometheus instance. Sign up for Grafana and not only do you get the application, but it comes with Prometheus out of the box.

1. Navigate to your Grafana instance welcome page. Click the blue button that says “Connect data.” 

[Insert picture of welcome page]

1. Select “Hosted Prometheus Metrics.”  

[Insert picture here]

1. Choose a method of forwarding your metrics.

[Insert picture here]

1. Use the Grafana Agent

[Insert picture here]

1. Choose your platform

[Insert picture here]

1. Download and install the binary for your OS

[Insert picture here]

1. Create an api key name and create a config file. 

[Insert picture here]

1. Grafana provides you code to create config file. The following is an example of that code. The Go application is running at localhost:8080 so add to the targets. Create the config file in the `observe-go` directory.

```
cat << EOF > ./agent-config.yaml
metrics:
  global:
    scrape_interval: 60s
  configs:
  - name: hosted-prometheus
    scrape_configs:
      - job_name: node
        static_configs:
        - targets: ['localhost:8080']
    remote_write:
      - url: https://prometheus-prod-10-prod-us-central-0.grafana.net/api/prom/push
        basic_auth:
          username: <grafana will give you this>
          password: <grafana will give you a password as well>
EOF
```

1. Run Grafana Agent with the config file.

`./agent-darwin-amd64 --config.file=agent-config.yaml`

## Visualize metrics in Grafana

1. Navigate to the Explore page.

[Insert picture here]

1. Select your Prometheus data source provided by Grafana Cloud.

[Insert picture here]

1. Select a metric to visualize. Using the metric select input, search for `observe_go_counter`, `observe_go_gauge`, and `observe_go_histogram`.

1. Run the query

[Insert picture]

Congratualations! You have created custom metrics!