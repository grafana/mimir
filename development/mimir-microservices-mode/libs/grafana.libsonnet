local prometheus = import 'prometheus.libsonnet';

{
  newGrafanaService:: {
    grafana: {
      image: 'grafana/grafana:12.1.1',
      environment: [
        'GF_AUTH_ANONYMOUS_ENABLED=true',
        'GF_AUTH_ANONYMOUS_ORG_ROLE=Admin',
      ],
      volumes: [
        './generated/datasources.yaml:/etc/grafana/provisioning/datasources/mimir.yaml',
        './generated/dashboards-mimir.yaml:/etc/grafana/provisioning/dashboards/mimir.yaml',
        '../../operations/mimir-mixin-compiled/dashboards:/var/lib/grafana/dashboards/Mimir',
      ],
      ports: ['3000:3000'],
    },
  },

  newGrafanaAgentService:: {
    // Scrape the metrics also with the Grafana agent (useful to test metadata ingestion
    // until metadata remote write is not supported by Prometheus).
    'grafana-agent': {
      image: 'grafana/agent:v0.37.3',
      command: ['-config.file=/etc/agent-config/grafana-agent.yaml', '-metrics.wal-directory=/tmp', '-server.http.address=127.0.0.1:9091'],
      volumes: ['./generated:/etc/agent-config'],
      ports: ['9091:9091'],
    },
  },

  newDatasourcesForConfig(config):: {
    apiVersion: 1,
    datasources: [
      {
        name: "Mimir",
        type: "prometheus",
        access: "proxy",
        uid: "mimir",
        orgID: 1,
        url: "http://envoy:%(port)d/prometheus" % {port: config.envoy.traffic_port},
        isDefault: true,
        jsonData: {
          prometheusType: "Mimir",
          exemplarTraceIdDestinations: [
            {
              name: "traceID",
              datasourceUid: "jaeger"
            }
          ]
        }
      },
      (if config.prometheus.enabled then {
        name: "Prometheus",
        type: "prometheus",
        access: "proxy",
        uid: "prometheus",
        orgID: 1,
        url: "http://prometheus:%(port)d" % {port: config.prometheus.port},
      } else {}),
      {
        name: "Mimir (with query result cache disabled)",
        type: "prometheus",
        access: "proxy",
        uid: "mimir-no-caching",
        orgID: 1,
        url: "http://envoy:%(port)d/prometheus" % {port: config.envoy.traffic_port},
        jsonData: {
          prometheusType: "Mimir",
          exemplarTraceIdDestinations: [
            {
              name: "traceID",
              datasourceUid: "jaeger"
            }
          ],
          httpHeaderName1: "Cache-Control"
        },
        secureJsonData: {
          httpHeaderValue1: "no-store"
        }
      },
      (if config.jaeger.enabled then {
        name: "Jaeger",
        type: "jaeger",
        access: "proxy",
        uid: "jaeger",
        orgID: 1,
        url: "http://jaeger:%(port)d/" % {port: config.jaeger.port},
      } else {}),
    ]
  },

  newAgentConfig(options):: {
    server: {
      log_level: "debug"
    },
    prometheus: {
      configs: [
        prometheus.newConfig({
          scrape_interval: "5s",
          write_target: 'http://%(host)s:%(port)s/api/v1/push' % {host: 'distributor-1', port: options.config.distributors.first_port},
          targets: options.targets,
          name: "grafana-agent",
        })
      ]
    }
  },

  mimirDashboards: {
    apiVersion: 1,
    providers: [{
      name: "Mimir",
      orgId: 1,
      type: "file",
      disableDeletion: true,
      updateIntervalSeconds: 10,
      allowUiUpdates: false,
      options: {
        path: "/var/lib/grafana/dashboards",
        foldersFromFilesStructure: true,
      }
    }]
  }
}