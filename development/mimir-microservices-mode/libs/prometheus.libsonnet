{
  newConfig(options)::: {
    global: {
      scrape_interval: options.scrape_interval,
      external_labels: {
        scraped_by: "prometheus",
      },
      always_scrape_classic_histograms: true,
    },

    scrape_configs: [
      {
        job_name: "mimir-microservices",
        static_configs: [
          {
            targets: options.targets,
            labels: {
              cluster: 'docker-compose',
              namespace: 'mimir-microservices-mode'
            }
          }
        ],
        relabel_configs: [
          {
            source_labels: ['__address__'],
            target_label: 'pod',
            regex: '([^:]+)(:[0-9]+)?',
            replacement: '${1}',
          },
          {
            source_labels: ['namespace', 'pod'],
            target_label: 'job',
            separator: '/',
            regex: '(.+?)(-\\d+)?',
            replacement: '${1}',
          },
          {
            source_labels: ['pod'],
            target_label: 'container',
            regex: '(.+?)(-\\d+)?',
            replacement: '${1}',
          }
        ]
      }
    ],

    remote_write: [
      {
        url: options.write_target,
        send_native_histograms: true,
        send_exemplars: true,
      }
    ],

    rule_files: [
      '/etc/mixin/mimir-alerts.yaml',
      '/etc/mixin/mimir-rules.yaml',
    ]
  }
}