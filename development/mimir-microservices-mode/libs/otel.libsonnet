local prometheus = import 'prometheus.libsonnet';

{
  newOtelCollectorService(options):: {
    otel_collector: {
      image: 'otel/opentelemetry-collector-contrib:0.88.0',
      command: ['--config=/etc/otel-collector/otel-collector.yaml'],
      volumes: ['./generated:/etc/otel-collector'],
      ports: ['%(port)d:%(port)d' % {port: options.config.otel_collector.port}],
    },
  },

  newOtelCollectorConfig(options):: {
    exporters: {
      prometheus: {
        endpoint: ':%d' % options.config.otel_collector.metrics_port
      },
    },

    processors: {
      batch: {}
    },

    receivers: {
      prometheus: {
        config: 
          prometheus.newConfig({
            scrape_interval: "5s",
            write_target: 'http://%(host)s:%(port)s/api/v1/push' % {host: 'distributor-1', port: options.config.distributors.first_port},
            targets: options.targets,
            name: "mimir-microservices-otel",
            rule_files: []
          })
      }
    }
  }
}