# e2ealerting

## How it works

`e2ealerting` helps measure the end to end alerting latency between [Prometheus](https://github.com/prometheus/prometheus) scrapes and [Alertmanager](https://github.com/prometheus/alertmanager) notification delivery. We achieve that by creating a metric that exposes the current time as its value, then creating an alert definition that uses that value as an annotation, and finally a built-in [Alertmanager](https://github.com/prometheus/alertmanager) webhook receiver observes the duration between the value of the annotation and webhook reception time as part of a Histogram.

A client for synchorinizing Prometheus rules definition and Alertmanager configuration allows us to experiment with different values directly within the tool without having to turn knobs configuration in different places. Keep in mind, that these are meant to only support the Cortex configuration API for the [Ruler](https://cortexmetrics.io/docs/api/#set-rule-group) and [Alertmanager](https://cortexmetrics.io/docs/api/#set-alertmanager-configuration).

Refer to the diagram below for a brief explanation on how this is all connected.

```
                                                    ┌─────────────────────────────────────┐
          ┌─────────────┐                           │  ┌─────────────┐       +-+-+-+-+-+-+│
          │ Prometheus  │                           │  │             │       |C|o|r|t|e|x|│
   ┌──────│     or      │──────Remote Write─────────┼─▶│ Remote Write│       +-+-+-+-+-+-+│
   │      │   Agent     │                           │  │ Endpoint    │                    │
   │      └─────────────┘                           │  └─────────────┘                    │
Scrape                                              │                                     │
   │                                                │                                     │
   │         ┌──────────Alert Notification Webhook──┼─────────┐                           │
   │         │                                      │         │                           │
   │         ▼                                      │         │                           │
   │  ┌─────────────┐                               │  ┌─────────────┐                    │
   │  │             │                               │  │             │                    │
   └─▶│ e2ealerting │──────Alertmanager Config──────┼─▶│ Alertmanager│                    │
      │             │                               │  │             │                    │
      └─────────────┘                               │  └─────────────┘                    │
             │                                      │  ┌─────────────┐                    │
             │                                      │  │             │                    │
             └──────────────Rule Group Config───────┼─▶│ Ruler       │                    │
                                                    │  │             │                    │
                                                    │  └─────────────┘                    │
                                                    └─────────────────────────────────────┘
```

## Installation

### Compiling from source

You can compile the binary directly by cloning this repo and running `make cmd/e2ealerting/e2ealerting`. The binary will be located in `cmd/e2ealerting/e2ealerting`.

### Docker Image

Docker images are part of [grafana/e2ealerting](https://hub.docker.com/repository/docker/grafana/e2ealerting/tags?page=1) in DockerHub.

## Configuration

An example `e2ealerting` configuration would look like:

```
-configs.key=<your-api-key>
-configs.alertmanager-file=/data/alertmanger_config.yaml
-configs.alertmanager-id=<alertmanager-id>
-configs.alertmanager-url=https://alertmanager-us-central1.grafana.net
-configs.rulegroup-file=/data/ruler_config.yaml
-configs.ruler-id=<prometheus-id>
-configs.ruler-url=https://prometheus-us-central1.grafana.net
-configs.sync-interval=2h
```

## [optional] Alertmanager and Ruler Configuration

Please note the both the `alertmanager` and `ruler` configuration are optional. If not defined as part of the config, the syncing process is disabled.

The Alertmanager configuration file `alertmanger_config.yaml` can be defined as:

```yaml
global:
receivers:
  - name: e2e-alerting
    webhook_configs:
      - url: <e2ealerting-url>/api/v1/receiver
route:
  group_interval: 1s
  group_wait: 1s
  receiver: e2e-alerting
  repeat_interval: 1s
```

Finally, the rule configuration file `ruler_config.yaml`:

```yaml
name: e2ealerting
rules:
  - alert: E2EAlertingAlwaysFiring
    annotations:
      time: "{{ $value }}"
    expr: e2ealerting_now_in_seconds > 0
    for: 10s
```
