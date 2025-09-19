{
  newPrometheusService(options):: {
    local defaults = {
      name: "prometheus",
      port: 9090,
      extraVolumes: []
    },

    local opts = defaults + options,

    [options.name]: {
      image: 'prom/prometheus:v3.5.0',
      command: [
        '--config.file=/etc/prometheus/%s.yaml' % opts.name,
        '--enable-feature=exemplar-storage',
        '--enable-feature=native-histograms',
      ],
      volumes: [
        './generated:/etc/prometheus',
      ] + opts.extraVolumes,
      ports: ['%d:9090' % opts.port],
    },
  },

  newConfig(configOptions):: ( 
    local defaultOptions = {
      scrape_interval: "5s",
      targets: [],
      name: "prometheus",
      write_target: "http://distributer-1:8000/api/v1/push",
    };

    local options = defaultOptions + configOptions;

    {
      global: {
        scrape_interval: options.scrape_interval,
        external_labels: {
          scraped_by: options.name,
        },
        always_scrape_classic_histograms: true,
      },

      scrape_configs: [$.scrapeConfigForTargets(options.name, options.targets)],

      remote_write: [
        {
          url: options.write_target,
          send_native_histograms: true,
          send_exemplars: true,
        }
      ],
    } + (if std.objectHas(options, 'rule_files') then {
      rule_files: options.rule_files
    } else {})
  ),

  scrapeConfigForTargets(name, targets):: {
    job_name: name,
    static_configs: [
      {
        targets: targets,
        labels: {
          cluster: 'docker-compose',
          namespace: 'mimir-microservices-mode'
        }
      }
    ],
    relabel_configs: $.common_relabelings
  },

  common_relabelings:: [
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